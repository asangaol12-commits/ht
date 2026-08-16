// ============================================================
// CLOUDFLARE WORKER + CALLS (SFU) + DURABLE OBJECT SIGNALING
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Content-Type": "application/json",
    };

    // 1. Handling CORS Preflight
    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    // Validasi Environment Variables
    if (!env.CLOUDFLARE_APP_ID || !env.CLOUDFLARE_CALLS_TOKEN) {
      return new Response(
        JSON.stringify({
          error: "Konfigurasi gagal: CLOUDFLARE_APP_ID atau CLOUDFLARE_CALLS_TOKEN belum diatur di Worker Secrets / Variables.",
        }),
        { status: 500, headers: corsHeaders }
      );
    }

    // Helper proxy request aman ke Cloudflare Calls API
    async function proxyToCalls(targetUrl, method, body = null) {
      try {
        const options = {
          method: method,
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_CALLS_TOKEN}`,
            "Content-Type": "application/json",
          },
        };
        if (body) options.body = body;

        const res = await fetch(targetUrl, options);
        const rawText = await res.text();

        let data;
        try {
          data = JSON.parse(rawText);
        } catch (e) {
          return new Response(
            JSON.stringify({
              error: `Cloudflare Calls API Error Status ${res.status}`,
              details: rawText,
            }),
            { status: res.status, headers: corsHeaders }
          );
        }

        return new Response(JSON.stringify(data), {
          status: res.status,
          headers: corsHeaders,
        });
      } catch (err) {
        return new Response(
          JSON.stringify({ error: err.message }),
          { status: 500, headers: corsHeaders }
        );
      }
    }

    // 2. PROXY REST API UNTUK CLOUDFLARE CALLS (SFU)

    // A. Endpoint: Create Session
    if (path === "/calls/session") {
      if (method === "POST") {
        const callsUrl = `https://rtc.live.cloudflare.com/v1/apps/${env.CLOUDFLARE_APP_ID}/sessions/new`;
        return proxyToCalls(callsUrl, "POST");
      }
      return new Response(
        JSON.stringify({ error: "Gunakan HTTP POST untuk membuat session di /calls/session" }),
        { status: 405, headers: corsHeaders }
      );
    }

    // B. Endpoint: Negotiate Tracks (/calls/session/:id/tracks/new)
    const trackMatch = path.match(/^\/calls\/session\/([^\/]+)\/tracks\/new$/);
    if (trackMatch) {
      if (method === "POST") {
        const sessionId = trackMatch[1];
        const callsUrl = `https://rtc.live.cloudflare.com/v1/apps/${env.CLOUDFLARE_APP_ID}/sessions/${sessionId}/tracks/new`;
        const body = await request.text();
        return proxyToCalls(callsUrl, "POST", body);
      }
      return new Response(
        JSON.stringify({ error: "Gunakan HTTP POST untuk meregister track" }),
        { status: 405, headers: corsHeaders }
      );
    }

    // 3. WEBSOCKET HANDLER (Mengarahkan ke Durable Object)
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader && upgradeHeader.toLowerCase() === "websocket") {
      const room = url.searchParams.get("room") || "default-room";
      let id = env.SIGNALING_ROOM.idFromName(room);
      let stub = env.SIGNALING_ROOM.get(id);
      return stub.fetch(request);
    }

    // 4. FALLBACK RESPONSE
    return new Response(
      JSON.stringify({
        status: "online",
        message: "Cloudflare Calls SFU Backend Running",
      }),
      { status: 200, headers: corsHeaders }
    );
  },
};

// ============================================================
// DURABLE OBJECT: SIGNALING ROOM
// ============================================================
export class SignalingRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

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
      return new Response(
        JSON.stringify({ error: "Expected Upgrade: websocket" }),
        { status: 426, headers: { "Content-Type": "application/json" } }
      );
    }

    const url = new URL(request.url);
    const requestedUser = url.searchParams.get("user");
    const clientId = (requestedUser && requestedUser.trim() !== "") ? requestedUser : "Anonymous";
    const uid = url.searchParams.get("uid") || "unknown-uid";
    const sessionId = url.searchParams.get("sessionId") || null;

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);
    server.serializeAttachment({ userId: clientId, uid: uid, sessionId: sessionId });

    this.broadcastRoomUsers();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(ws, msg) {
    try {
      let messageStr = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
      let data = JSON.parse(messageStr);
      let sockets = this.state.getWebSockets();

      if (data.type === "set_session_id") {
        let meta = ws.deserializeAttachment() || {};
        meta.sessionId = data.sessionId;
        ws.serializeAttachment(meta);
        this.broadcastRoomUsers();
        return;
      }

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
    this.broadcastRoomUsers();
    try {
      ws.close(code, "Closed by server");
    } catch (e) {}
  }

  async webSocketError(ws, error) {
    this.broadcastRoomUsers();
  }
}
