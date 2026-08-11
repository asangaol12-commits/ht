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
    this.socketIds = new Map(); // Mapping dari socket -> clientId
  }

  // Fungsi helper untuk mengirim daftar user terbaru ke SEMUA client di room
  broadcastRoomUsers() {
    let sockets = this.state.getWebSockets();
    let usersList = [];
    
    // Kumpulkan semua ID user yang unik/aktif
    for (let socket of sockets) {
      let userId = this.socketIds.get(socket);
      if (userId) {
        usersList.push(userId);
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
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "user";

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    this.socketIds.set(server, clientId);
    console.log(`[CONNECT] Klien terhubung dengan User ID: ${clientId}`);

    // Update dan broadcast daftar user terbaru setelah ada yang masuk
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
            // Jika pesan memiliki target khusus (seperti offer, answer, candidate)
            if (data.target) {
              let targetUserId = this.socketIds.get(socket);
              // Kirim HANYA ke socket yang user ID-nya sesuai dengan target
              if (targetUserId === data.target) {
                socket.send(JSON.stringify(data));
              }
            } else {
              // Untuk pesan broadcast umum tanpa target (press, release, join, dll)
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
    let senderId = this.socketIds.get(server);
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    this.socketIds.delete(server);
    
    // Update dan broadcast daftar user terbaru setelah ada yang keluar
    this.broadcastRoomUsers();
    
    server.close(code, "Closed by server");
  }

  async webSocketError(server, error) {
    let senderId = this.socketIds.get(server);
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    this.socketIds.delete(server);
    
    // Update dan broadcast daftar user terbaru jika terjadi error koneksi
    this.broadcastRoomUsers();
  }
}
