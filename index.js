const rooms = new Map();
export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const url = new URL(request.url);
    const roomId = url.searchParams.get("room") || "default-room";
    const userId = url.searchParams.get("user") || "Anonymous";
    const uniqueUid = url.searchParams.get("uid") || "";

    // Terima koneksi WebSocket menggunakan WebSockets API Cloudflare
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();

    // Attach metadata ke objek server WebSocket agar mudah dikenali
    server.meta = {
      userId: userId,
      uid: uniqueUid,
      roomId: roomId,
    };

    // Tambahkan client ke dalam room yang sesuai
    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Set());
    }
    rooms.get(roomId).add(server);

    console.log(`[CONNECT] User: ${userId} (UID: ${uniqueUid}) bergabung ke Room: ${roomId}`);
    
    // Broadcast daftar terbaru user ke semua peserta di room tersebut
    broadcastRoomUsers(roomId);

    // Event handler ketika menerima pesan dari aplikasi Android client
    server.addEventListener("message", (event) => {
      try {
        const rawData = event.data;
        const msg = JSON.parse(rawData);
        const targetUser = msg.target; // Jika ada target spesifik (untuk WebRTC P2P)

        // Handle pesan press / release / offer / answer / candidate
        const roomClients = rooms.get(roomId);
        if (!roomClients) return;

        for (let clientSocket of roomClients) {
          // Jangan kirim balik pesan ke pengirimnya sendiri
          if (clientSocket === server) continue;

          // Jika pesan bersifat targeted (seperti SDP offer/answer/candidate yang punya field target)
          if (targetUser) {
            if (clientSocket.meta.userId === targetUser || clientSocket.meta.uid === targetUser) {
              clientSocket.send(rawData);
              break;
            }
          } else {
            // Jika pesan broadcast umum (seperti "press" atau "release" PTT ke seluruh room)
            clientSocket.send(rawData);
          }
        }
      } catch (err) {
        console.error(`[ERROR] Gagal memproses pesan WebSocket:`, err);
      }
    });

    // Event handler ketika koneksi terputus (Close / Error)
    const cleanup = () => {
      console.log(`[DISCONNECT] User: ${userId} keluar dari Room: ${roomId}`);
      const roomClients = rooms.get(roomId);
      if (roomClients) {
        roomClients.delete(server);
        if (roomClients.size === 0) {
          rooms.delete(roomId); // Hapus room jika kosong untuk menghemat memori
        } else {
          broadcastRoomUsers(roomId); // Perbarui daftar user untuk peserta yang tersisa
        }
      }
    };

    server.addEventListener("close", cleanup);
    server.addEventListener("error", cleanup);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

// Fungsi helper untuk mengirim daftar user aktif dalam satu room ke semua client terkait
function broadcastRoomUsers(roomId) {
  const roomClients = rooms.get(roomId);
  if (!roomClients) return;

  const usersList = [];
  for (let clientSocket of roomClients) {
    if (clientSocket.meta && clientSocket.meta.userId) {
      if (!usersList.includes(clientSocket.meta.userId)) {
        usersList.push(clientSocket.meta.userId);
      }
    }
  }

  const payload = JSON.stringify({
    type: "room_users",
    users: usersList,
  });

  for (let clientSocket of roomClients) {
    try {
      clientSocket.send(payload);
    } catch (e) {
      console.error(`[BROADCAST ERROR] Gagal mengirim room_users ke client:`, e);
    }
  }
}
