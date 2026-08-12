export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const url = new URL(request.url);
    const roomId = url.searchParams.get("room") || "default-room";

    // Arahkan request WebSocket ke Durable Object berdasarkan Nama/ID Room
    let id = env.SignalingRoom.idFromName(roomId);
    let stub = env.SignalingRoom.get(id);

    return stub.fetch(request);
  }
};

// Definisi Durable Object untuk menghandle Room & WebSocket secara Stateful
export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();
  }

  async fetch(request) {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();

    const url = new URL(request.url);
    const userId = url.searchParams.get("user") || "Anonymous";
    const uniqueUid = url.searchParams.get("uid") || "";
    const roomId = url.searchParams.get("room") || "default-room";

    server.meta = { userId, uid: uniqueUid, roomId };
    this.sessions.add(server);

    console.log(`[CONNECT] User: ${userId} bergabung ke Durable Room: ${roomId}`);
    this.broadcastRoomUsers();

    server.addEventListener("message", (event) => {
      try {
        const rawData = event.data;
        const msg = JSON.parse(rawData);
        const targetUser = msg.target;

        for (let clientSocket of this.sessions) {
          if (clientSocket === server) continue;

          if (targetUser) {
            if (clientSocket.meta.userId === targetUser || clientSocket.meta.uid === targetUser) {
              clientSocket.send(rawData);
              break;
            }
          } else {
            clientSocket.send(rawData);
          }
        }
      } catch (err) {
        console.error(`[ERROR] Gagal memproses pesan:`, err);
      }
    });

    const cleanup = () => {
      console.log(`[DISCONNECT] User: ${userId} keluar dari room.`);
      this.sessions.delete(server);
      this.broadcastRoomUsers();
    };

    server.addEventListener("close", cleanup);
    server.addEventListener("error", cleanup);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  broadcastRoomUsers() {
    const usersList = [];
    for (let clientSocket of this.sessions) {
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

    for (let clientSocket of this.sessions) {
      try {
        clientSocket.send(payload);
      } catch (e) {
        console.error(`[BROADCAST ERROR]`, e);
      }
    }
  }
}
