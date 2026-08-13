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
    let usersList = [];
    
    for (let socket of sockets) {
      let tags = this.state.getTags(socket);
      if (tags && tags.length > 0) {
        usersList.push(tags[0]);
      }
    }

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
    let baseClientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser.trim() : "user";

    // Pencegahan duplikasi nama user dalam satu room
    let sockets = this.state.getWebSockets();
    let existingUsers = new Set();
    for (let socket of sockets) {
      let tags = this.state.getTags(socket);
      if (tags && tags[0]) {
        existingUsers.add(tags[0]);
      }
    }

    let clientId = baseClientId;
    let counter = 1;
    while (existingUsers.has(clientId)) {
      counter++;
      clientId = `${baseClientId}_${counter}`;
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Menyimpan clientId ke dalam tag WebSocket Durable Object
    this.state.acceptWebSocket(server, [clientId]);

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
      let data;
      
      try {
        data = JSON.parse(messageStr);
      } catch (parseErr) {
        console.warn("[WARNING] Menerima pesan bukan JSON yang valid:", messageStr);
        return;
      }

      let sockets = this.state.getWebSockets();
      let targetUser = data.target;

      for (let socket of sockets) {
        if (socket !== server) {
          try {
            if (targetUser) {
              let tags = this.state.getTags(socket);
              let recipientId = tags && tags.length > 0 ? tags[0] : null;
              
              if (recipientId === targetUser) {
                socket.send(JSON.stringify(data));
                break; // Target ketemu, stop perulangan
              }
            } else {
              socket.send(JSON.stringify(data));
            }
          } catch (e) {
            console.error("[ERROR] Gagal kirim pesan ke socket:", e);
          }
        }
      }
    } catch (err) {
      console.error("[ERROR] Terjadi kesalahan pada webSocketMessage:", err);
    }
  }

  async webSocketClose(server, code, reason, wasClean) {
    let tags = this.state.getTags(server);
    let senderId = tags && tags.length > 0 ? tags[0] : "unknown";
    
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    
    this.broadcastRoomUsers();
    
    try {
      server.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(server, error) {
    let tags = this.state.getTags(server);
    let senderId = tags && tags.length > 0 ? tags[0] : "unknown";
    
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    
    this.broadcastRoomUsers();
  }
}
