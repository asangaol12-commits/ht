export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('WebRTC Signaling Server is running!', { status: 200 });
    }

    const room = url.searchParams.get('room') || 'default-room';
    let id = env.SIGNALING_ROOM.idFromName(room);
    let stub = env.SIGNALING_ROOM.get(id);
    
    return stub.fetch(request);
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    // Menggunakan tag/metadata bawaan Cloudflare untuk menyimpan ID socket
    this.clientIds = new Map();
    this.counter = 1;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Menerima WebSocket di Durable Object
    this.state.acceptWebSocket(server);

    const clientId = "user_" + (this.counter++);
    this.clientIds.set(server, clientId);
    console.log(`[CONNECT] Klien terhubung: ${clientId}`);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(server, msg) {
    try {
      console.log(`[MESSAGE] Menerima pesan mentah:`, msg);
      
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      let data = JSON.parse(messageStr);
      
      let senderId = this.clientIds.get(server);
      if (!senderId) {
        senderId = "unknown";
      }

      // Sisipkan pengirim
      data.from = senderId;

      let sockets = this.state.getWebSockets();
      console.log(`[BROADCAST] Mengirim ke ${sockets.length} client di room ini.`);

      // Broadcast ke semua client lain
      for (let socket of sockets) {
        if (socket !== server) {
          try {
            socket.send(JSON.stringify(data));
          } catch (e) {
            console.error("[ERROR] Gagal kirim ke socket:", e);
          }
        }
      }
    } catch (err) {
      console.error("[ERROR] Gagal parsing/proses pesan:", err);
    }
  }

  async webSocketClose(server, code, reason, wasClean) {
    let senderId = this.clientIds.get(server);
    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    this.clientIds.delete(server);
  }

  async webSocketError(server, error) {
    console.error("[ERROR] WebSocket error:", error);
    this.clientIds.delete(server);
  }
}
