export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    const upgradeHeader = request.headers.get('Upgrade');

    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
      const room = url.searchParams.get('room') || 'default-room';
      let id = env.SIGNALING_ROOM.idFromName(room);
      let stub = env.SIGNALING_ROOM.get(id);
      return stub.fetch(request);
    }

    return new Response('not-found', { status: 200 });
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env; 
  }

  // Fungsi helper untuk mengambil semua socket aktif dalam mode hibernasi
  broadcastRoomUsers() {
    let sockets = this.state.getWebSockets();
    let usersSet = new Set(); 
    
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
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "Anonymous";
    const uid = url.searchParams.get('uid') || "unknown-uid";

    console.log(`[CONFIG] Menggunakan App ID: ${this.env.CLOUDFLARE_APP_ID}`);

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Mengaktifkan WebSocket dengan opsi hibernasi
    this.state.acceptWebSocket(server);

    // Menyimpan metadata ke attachment socket
    server.serializeAttachment({ userId: clientId, uid: uid });
    
    console.log(`[CONNECT] Klien terhubung - User ID: ${clientId} | UID: ${uid}`);

    this.broadcastRoomUsers();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  // Handler khusus untuk Hibernatable WebSockets
  async webSocketMessage(ws, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      let data = JSON.parse(messageStr);
      let sockets = this.state.getWebSockets();

      for (let socket of sockets) {
        if (socket !== ws) {
          try {
            let meta = socket.deserializeAttachment();
            let targetUserId = meta ? meta.userId : null;
            let targetUid = meta ? meta.uid : null;

            if (data.target) {
              if (data.target === targetUserId || data.target === targetUid) {
                socket.send(messageStr);
              }
            } else {
              socket.send(messageStr);
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

  async webSocketClose(ws, code, reason, wasClean) {
    let meta = ws.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    this.broadcastRoomUsers();
    
    try {
      ws.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(ws, error) {
    let meta = ws.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    this.broadcastRoomUsers();
  }
}
