export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    // Langsung tangani jika itu permintaan WebSocket (tanpa harus cek pathname /ws)
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
