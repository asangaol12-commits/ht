export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
      const room = url.searchParams.get('room') || 'default-room';
      let id = env.SIGNALING_ROOM.idFromName(room);
      let stub = env.SIGNALING_ROOM.get(id);
      return stub.fetch(request);
    }

    return new Response('not found', { status: 200 });
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
  }

  broadcastRoomUsers() {
    let sockets = this.state.getWebSockets();
    let usersSet = new Set(); // Pakai Set agar ID user tidak ganda
    
    for (let socket of sockets) {
      try {
        let meta = socket.deserializeAttachment();
        if (meta && meta.userId) {
          usersSet.add(meta.userId);
        }
      } catch (e) {
        console.error("[ERROR] Gagal membaca attachment socket:", e);
      }
    }

    let usersList = Array.from(usersSet);

    let payload = JSON.stringify({
      type: "room_users",
      users: usersList
    });

    for (let socket of sockets) {
      try {
        socket.send(payload);
      } catch (e) {
        console.error("[ERROR] Gagal kirim room_users:", e);
      }
    }
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const url = new URL(request.url);
    const requestedUser = url.searchParams.get('user');
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "user";

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    server.serializeAttachment({ userId: clientId });
    
    console.log(`[CONNECT] Klien terhubung dengan User ID: ${clientId}`);

    this.broadcastRoomUsers();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(server, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      let data = JSON.parse(messageStr);
      let sockets = this.state.getWebSockets();

      for (let socket of sockets) {
        if (socket !== server) {
          try {
            let meta = socket.deserializeAttachment();
            let targetUserId = meta ? meta.userId : null;

            if (data.target) {
              if (targetUserId === data.target) {
                socket.send(JSON.stringify(data));
              }
            } else {
              // Pesan broadcast (press, release, dll)
              socket.send(JSON.stringify(data));
            }
          } catch (e) {
            console.error("[ERROR] Gagal kirim pesan ke socket:", e);
          }
        }
      }
    } catch (err) {
      console.error("[ERROR] Gagal parsing JSON:", err);
    }
  }

  async webSocketClose(server, code, reason, wasClean) {
    let meta = server.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    
    this.broadcastRoomUsers();
    
    try {
      server.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(server, error) {
    let meta = server.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    
    this.broadcastRoomUsers();
  }
}
