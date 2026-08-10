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

    return new Response('WebRTC Walkie-Talkie Signaling Server is running!', { status: 200 });
  }
};

export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    this.socketIds = new Map();
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const url = new URL(request.url);
    const requestedUser = url.searchParams.get('user');
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "anonymous";

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    this.socketIds.set(server, clientId);
    console.log(`[CONNECT] Klien Walkie-Talkie terhubung: ${clientId}`);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(server, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      
      // Batasi ukuran payload signaling agar tetap ringan (maksimal 4KB untuk teks/state PTT)
      if (messageStr.length > 4096) {
        return;
      }

      let data = JSON.parse(messageStr);
      let sockets = this.state.getWebSockets();
      
      if (data.userId && data.userId.trim() !== "" && data.userId !== "anonymous") {
        this.socketIds.set(server, data.userId.trim());
      }

      let senderId = this.socketIds.get(server) || "anonymous";
      data.from = senderId;

      // Filter ketat: Hanya izinkan signaling WebRTC dasar & state PTT (Push-to-Talk / Press)
      const validTypes = ['offer', 'answer', 'candidate', 'join', 'leave', 'ptt-press', 'ptt-release'];
      if (data.type && !validTypes.includes(data.type)) {
        return; // Abaikan pesan di luar audio/PTT
      }

      let payload = JSON.stringify(data);

      // Broadcast status press/release atau signaling audio ke client lain di room
      for (let socket of sockets) {
        if (socket !== server) {
          try {
            socket.send(payload);
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
    console.log(`[DISCONNECT] Klien Walkie-Talkie terputus: ${senderId}`);
    this.socketIds.delete(server);
    server.close(code, "Closed by server");
  }

  async webSocketError(server, error) {
    let senderId = this.socketIds.get(server);
    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    this.socketIds.delete(server);
  }
}
