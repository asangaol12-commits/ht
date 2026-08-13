export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 1. Logika CORS Preflight untuk browser/klien web (jika dibutuhkan)
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

    // 2. Teruskan koneksi WebSocket ke Durable Object (SignalingRoom)
    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
      const room = url.searchParams.get('room') || 'default-room';
      let id = env.SIGNALING_ROOM.idFromName(room);
      let stub = env.SIGNALING_ROOM.get(id);
      return stub.fetch(request);
    }

    // Response default jika tidak menggunakan WebSocket
    return new Response('not-found', { status: 200 });
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
  }

  broadcastRoomUsers() {
    let sockets = this.state.getWebSockets();
    let usersSet = new Set(); 
    
    // Ambil semua userId yang sedang terkoneksi
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

    // Broadcast daftar user ke semua klien di room ini
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
    
    // 3. Logika parsing UID dan UserId dari URL params
    const requestedUser = url.searchParams.get('user');
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "Anonymous";
    const uid = url.searchParams.get('uid') || "unknown-uid";

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    // 4. Menyimpan *userId* DAN *uid* ke dalam state connection
    server.serializeAttachment({ userId: clientId, uid: uid });
    
    console.log(`[CONNECT] Klien terhubung - User ID: ${clientId} | UID: ${uid}`);

    // Update list partisipan untuk semua member
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
        // Jangan kirim kembali pesan ke pengirim
        if (socket !== server) {
          try {
            let meta = socket.deserializeAttachment();
            let targetUserId = meta ? meta.userId : null;
            let targetUid = meta ? meta.uid : null;

            // 5. Logika Smart Routing: Jika ada target, arahkan khusus (Offer, Answer, Candidate)
            if (data.target) {
              if (data.target === targetUserId || data.target === targetUid) {
                socket.send(messageStr);
              }
            } else {
              // 6. Jika tidak ada target, broadcast ke seluruh room (Press, Release)
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

  async webSocketClose(server, code, reason, wasClean) {
    let meta = server.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    
    // Perbarui member list ke orang yang tersisa di room
    this.broadcastRoomUsers();
    
    try {
      server.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(server, error) {
    let meta = server.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";
    
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    
    // Perbarui member list
    this.broadcastRoomUsers();
  }
}
