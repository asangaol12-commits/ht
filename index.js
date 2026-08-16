export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 1. Logika CORS Preflight untuk browser/klien web
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
    this.env = env; // <--- PERUBAHAN UTAMA: Menyimpan env agar bisa dibaca di seluruh method class ini!
  }

  // Contoh fungsi opsional untuk mengambil token/sesi otomatis dari RealtimeKit
  async createRealtimeSession() {
    try {
      const response = await fetch(`https://rtc.live.cloudflare.com/v1/apps/${this.env.CLOUDFLARE_APP_ID}/sessions/new`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${this.env.CLOUDFLARE_API_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });
      if (!response.ok) return null;
      return await response.json();
    } catch (e) {
      console.error("[ERROR] Gagal membuat session RealtimeKit:", e);
      return null;
    }
  }

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

    // Contoh penggunaan Secret/Variable: 
    // Anda bisa mengakses `this.env.CLOUDFLARE_APP_ID` atau `this.env.NAMA_VARIABEL` di sini kapan saja.
    console.log(`[CONFIG] Menggunakan App ID: ${this.env.CLOUDFLARE_APP_ID}`);

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    server.serializeAttachment({ userId: clientId, uid: uid });
    
    console.log(`[CONNECT] Klien terhubung - User ID: ${clientId} | UID: ${uid}`);

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
