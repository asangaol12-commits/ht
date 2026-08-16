// ============================================================
// CLOUDFLARE WORKER + CALLS (SFU) + DURABLE OBJECT SIGNALING
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 1. Handling CORS Preflight untuk Akses dari Aplikasi Android / Web
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json",
    };

    // 2. PROXY REST API UNTUK CLOUDFLARE CALLS (SFU)
    // Menjaga agar CLOUDFLARE_CALLS_TOKEN tetap aman di Worker

    // A. Endpoint: Membuat Session Baru di Cloudflare Calls
    // Client memanggil: POST /calls/session
    if (url.pathname === "/calls/session" && request.method === "POST") {
      const callsUrl = `https://rtc.live.cloudflare.com/v1/apps/${env.CLOUDFLARE_APP_ID}/sessions/new`;
      
      try {
        const res = await fetch(callsUrl, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_CALLS_TOKEN}`,
            "Content-Type": "application/json",
          },
        });
        const data = await res.json();
        return new Response(JSON.stringify(data), { status: res.status, headers: corsHeaders });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: corsHeaders });
      }
    }

    // B. Endpoint: Negosiasi Track (Publish Local Track / Subscribe Remote Track)
    // Client memanggil: POST /calls/session/:sessionId/tracks/new
    const trackMatch = url.pathname.match(/^\/calls\/session\/([^\/]+)\/tracks\/new$/);
    if (trackMatch && request.method === "POST") {
      const sessionId = trackMatch[1];
      const callsUrl = `https://rtc.live.cloudflare.com/v1/apps/${env.CLOUDFLARE_APP_ID}/sessions/${sessionId}/tracks/new`;

      try {
        const body = await request.text();
        const res = await fetch(callsUrl, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_CALLS_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: body,
        });
        const data = await res.json();
        return new Response(JSON.stringify(data), { status: res.status, headers: corsHeaders });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: corsHeaders });
      }
    }

    // 3. WEBSOCKET HANDLER (Untuk State Room & Broadcast Peserta)
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader && upgradeHeader.toLowerCase() === "websocket") {
      const room = url.searchParams.get("room") || "default-room";
      let id = env.SIGNALING_ROOM.idFromName(room);
      let stub = env.SIGNALING_ROOM.get(id);
      return stub.fetch(request);
    }

return new Response(JSON.stringify({
  status: "online",
  message: "Cloudflare Calls SFU Backend Running"
}), { status: 200, headers: corsHeaders });
  }
};

// ============================================================
// DURABLE OBJECT: SIGNALING ROOM (ROOM MANAGEMENT & BROADCAST)
// ============================================================
export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  // Broadcast daftar user & Calls sessionId aktif di room
  broadcastRoomUsers() {
    let sockets = this.state.getWebSockets();
    let users = [];

    for (let socket of sockets) {
      try {
        let meta = socket.deserializeAttachment();
        if (meta && meta.userId) {
          users.push({
            userId: meta.userId,
            uid: meta.uid,
            sessionId: meta.sessionId || null,
          });
        }
      } catch (e) {
        console.error("[ERROR] Gagal membaca attachment socket:", e);
      }
    }

    let payload = JSON.stringify({
      type: "room_users",
      users: users,
    });

    for (let socket of sockets) {
      try {
        socket.send(payload);
      } catch (e) {
        console.error("[ERROR] Gagal kirim room_users:", e);
      }
    }
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const url = new URL(request.url);
    const requestedUser = url.searchParams.get("user");
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "Anonymous";
    const uid = url.searchParams.get("uid") || "unknown-uid";
    const sessionId = url.searchParams.get("sessionId") || null; // ID Sesi Cloudflare Calls jika ada

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Mengaktifkan mode hibernasi
    this.state.acceptWebSocket(server);

    // Simpan metadata ke attachment socket
    server.serializeAttachment({ userId: clientId, uid: uid, sessionId: sessionId });

    console.log(`[CONNECT] Klien: ${clientId} | UID: ${uid} | Calls Session: ${sessionId}`);

    this.broadcastRoomUsers();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  // Handler pesan WebSocket dari Klien (Hibernatable API)
  async webSocketMessage(ws, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      let data = JSON.parse(messageStr);
      let sockets = this.state.getWebSockets();

      // Jika klien memperbarui Session ID Cloudflare Calls mereka
      if (data.type === "set_session_id") {
        let meta = ws.deserializeAttachment() || {};
        meta.sessionId = data.sessionId;
        ws.serializeAttachment(meta);
        this.broadcastRoomUsers();
        return;
      }

      // Forward pesan (misal notification `track_published`, chat, dll) ke user lain
      for (let socket of sockets) {
        if (socket !== ws) {
          try {
            let meta = socket.deserializeAttachment();
            let targetUserId = meta ? meta.userId : null;
            let targetUid = meta ? meta.uid : null;

            if (data.target) {
              if (data.target === targetUserId || data.target === targetUid) {
                socket.send(messageStr);
              }
            } else {
              socket.send(messageStr);
            }
          } catch (e) {
            console.error("[ERROR] Gagal kirim pesan ke socket:", e);
          }
        }
      }
    } catch (err) {
      console.error("[ERROR] Gagal parsing JSON:", err);
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    let meta = ws.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";

    console.log(`[DISCONNECT] Klien terputus: ${senderId}`);
    this.broadcastRoomUsers();

    try {
      ws.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(ws, error) {
    let meta = ws.deserializeAttachment();
    let senderId = meta ? meta.userId : "unknown";

    console.error(`[ERROR] WebSocket error pada ${senderId}:`, error);
    this.broadcastRoomUsers();
  }
}
