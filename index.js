export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
      const room = url.searchParams.get('room') || 'default-room';
      console.log(`[HTTP] Permintaan WebSocket masuk untuk Room: ${room}`);
      
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
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // DAFTARKAN SOCKET DENGAN TAG "room-client" AGAR BISA DIAMBIL KEMBALI
    this.state.acceptWebSocket(server, ["room-client"]);
    console.log(`[CONNECT] Klien baru berhasil terhubung ke Room.`);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(server, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      console.log("[MESSAGE] Pesan diterima:", messageStr);

      let data = JSON.parse(messageStr);
      
      // AMBIL SEMUA SOCKET BERDASARKAN TAG YANG TELAH DIDAFTARKAN
      let sockets = this.state.getWebSockets("room-client");

      console.log(`[BROADCAST] Meneruskan pesan ke ${sockets.length - 1} klien lain di room.`);

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
      console.error("[ERROR] Gagal parsing JSON / memproses pesan:", err, "Isi msg:", msg);
    }
  }

  async webSocketClose(server, code, reason, wasClean) {
    console.log(`[DISCONNECT] Klien terputus. Code: ${code}, Alasan: ${reason}`);
  }

  async webSocketError(server, error) {
    console.error("[ERROR] WebSocket error terjadi:", error);
  }
}
