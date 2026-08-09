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
    // Menyimpan mapping socket ke ID unik (agar setiap client punya 'from' ID)
    this.socketIds = new Map();
    this.counter = 1;
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    // Berikan ID unik ke setiap client yang terhubung
    const clientId = "user_" + (this.counter++);
    this.socketIds.set(server, clientId);
    console.log(`[CONNECT] Klien baru terhubung dengan ID: ${clientId}`);

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
      
      // Ambil ID pengirim, jika tidak ada (karena instance restart), buatkan secara otomatis
      let senderId = this.socketIds.get(server);
      if (!senderId) {
        senderId = "user_" + (this.counter++);
        this.socketIds.set(server, senderId);
      }

      // WAJIB: Sisipkan field 'from' agar HP penerima tahu pesan ini dari siapa
      data.from = senderId;

      // Broadcast pesan ke SEMUA client lain di room yang sama
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
    let senderId = this.socketIds.get(server);
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    this.socketIds.delete(server);
    server.close(code, "Closed by server");
  }

  async webSocketError(server, error) {
    let senderId = this.socketIds.get(server);
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    this.socketIds.delete(server);
  }
}
