export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (url.pathname === '/ws' || (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket')) {
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
    // Memakai fitur state.getWebSockets() bawaan Cloudflare Durable Object agar lebih stabil
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Menerima WebSocket menggunakan state dari Cloudflare (Durable Object Hibernation compatible)
    this.state.acceptWebSocket(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(server, msg) {
    try {
      let data = JSON.parse(msg);
      
      // Ambil semua WebSocket yang terhubung ke room ini
      let sockets = this.state.getWebSockets();

      // Broadcast pesan signaling ke SEMUA peer lain di room yang sama
      for (let socket of sockets) {
        if (socket !== server) {
          try {
            socket.send(JSON.stringify(data));
          } catch (e) {
            console.error("Gagal kirim pesan ke socket:", e);
          }
        }
      }
    } catch (err) {
      console.error("Gagal parsing JSON:", err);
    }
  }

  async webSocketClose(server, code, reason, wasClean) {
    server.close(code, "Closed by server");
  }

  async webSocketError(server, error) {
    console.error("WebSocket error:", error);
  }
}
