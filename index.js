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

    return new Response('WebRTC Signaling Server is running!', { status: 200 });
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    // Menyimpan socket siapa yang sedang aktif bicara (Floor Control)
    this.currentSpeaker = null;
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);
    console.log(`[CONNECT] Klien baru berhasil terhubung ke Room.`);

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

      // FLOOR CONTROL LOGIC:
      // Jika pesan berupa "press" atau "offer", artinya client ini mau mulai bicara (request mic)
      if (data.type === "press" || data.type === "offer") {
        if (this.currentSpeaker === null) {
          this.currentSpeaker = server;
          console.log("[FLOOR] Speaker dikunci oleh klien baru.");
        } else if (this.currentSpeaker !== server) {
          // Kalau sudah ada orang lain yang ngomong, abaikan
          console.log("[FLOOR] Ditolak: Saluran sedang digunakan orang lain.");
          return; 
        }
      }

      // Jika client melepas PTT / selesai bicara
      if (data.type === "release") {
        if (this.currentSpeaker === server) {
          this.currentSpeaker = null;
          console.log("[FLOOR] Saluran dibebaskan (Mic dilepas).");
        }
        // Tanpa return agar pesan release ikut ter-broadcast ke socket lain
      }

      // Broadcast normal untuk candidate/answer/offer/release yang diizinkan
      for (let socket of sockets) {
        if (socket !== server) {
          try {
            socket.send(JSON.stringify(data));
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
    console.log(`[DISCONNECT] Klien terputus.`);
    // Kalau yang putus adalah speaker aktif, bebaskan salurannya
    if (this.currentSpeaker === server) {
      this.currentSpeaker = null;
    }
    server.close(code, "Closed by server");
  }

  async webSocketError(server, error) {
    console.error("[ERROR] WebSocket error:", error);
    if (this.currentSpeaker === server) {
      this.currentSpeaker = null;
    }
  }
}
