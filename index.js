export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // Validasi apakah request berupa WebSocket
    if (url.pathname === '/ws') {
      const room = url.searchParams.get('room') || 'default-room';
      
      // Arahkan koneksi ke Durable Objects berdasarkan ID Room
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
    this.sessions = [];
  }

  async fetch(request) {
    // Upgrade koneksi HTTP biasa menjadi WebSocket
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    
    let session = { socket: server };
    this.sessions.push(session);

    server.addEventListener('message', async (msg) => {
      try {
        let data = JSON.parse(msg.data);
        
        // Broadcast pesan signaling ke SEMUA peer lain di room yang sama
        this.sessions.forEach(s => {
          if (s.socket !== server) {
            s.socket.send(JSON.stringify(data));
          }
        });
      } catch (err) {
        console.error("Gagal parsing JSON:", err);
      }
    });

    server.addEventListener('close', () => {
      this.sessions = this.sessions.filter(s => s !== session);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
}
