const { WebSocketServer } = require("ws");

// 1. DYNAMIC PORT: Required for Cloud Deployment (Render/Railway)
const PORT = process.env.PORT || 3000;
const wss = new WebSocketServer({ port: PORT });

// roomId -> Map(username -> { currentProblem, sockets: Set(ws) })
const rooms = new Map(); 

function broadcastRoomPresence(roomId) {
  if (!rooms.has(roomId)) return;
  
  const room = rooms.get(roomId);
  const members = [];
  
  for (const [username, data] of room.entries()) {
    members.push({
      username: username,
      currentProblem: data.currentProblem
    });
  }

  const message = JSON.stringify({ type: "ROOM_PRESENCE", members });

  for (const data of room.values()) {
    for (const client of data.sockets) {
      if (client.readyState === 1) { // 1 === OPEN
        client.send(message);
      }
    }
  }
}

wss.on("connection", (ws) => {
  let userRoom = null;
  let userName = null;

  // 2. ERROR HANDLER: Prevents random socket drops from crashing the whole server
  ws.on("error", console.error);

  // 3. HEARTBEAT INIT: Mark connection as alive
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    try {
      const data = JSON.parse(raw);

      if (data.type === "JOIN_ROOM") {
        const { roomId, username } = data;
        userRoom = roomId;
        userName = username || "Anonymous";

        if (!rooms.has(roomId)) {
          rooms.set(roomId, new Map());
        }

        const room = rooms.get(roomId);
        
        if (!room.has(userName)) {
          room.set(userName, { 
            currentProblem: "Viewing sheet", 
            sockets: new Set() 
          });
        }

        room.get(userName).sockets.add(ws);

        console.log(`🟢 ${userName} joined room: ${roomId} (Tabs open: ${room.get(userName).sockets.size})`);
        broadcastRoomPresence(roomId);
      }

      if (data.type === "SET_PROBLEM" && userRoom && userName && rooms.has(userRoom)) {
        const room = rooms.get(userRoom);
        if (room.has(userName)) {
          room.get(userName).currentProblem = data.problemName;
          broadcastRoomPresence(userRoom);
        }
      }
    } catch (err) {
      console.error("Message parse error:", err);
    }
  });

  ws.on("close", () => {
    if (userRoom && userName && rooms.has(userRoom)) {
      const room = rooms.get(userRoom);
      
      if (room.has(userName)) {
        const userData = room.get(userName);
        
        userData.sockets.delete(ws);
        
        if (userData.sockets.size === 0) {
          room.delete(userName);
          console.log(`🔴 ${userName} left room: ${userRoom}`);
        }
      }
      
      if (room.size === 0) {
        rooms.delete(userRoom); 
      } else {
        broadcastRoomPresence(userRoom); 
      }
    }
  });
});

// 4. HEARTBEAT INTERVAL: Ping all clients every 30 seconds to prevent cloud disconnects
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => {
  clearInterval(interval);
});

console.log(`🟢 WebSocket Study Room Server running on port ${PORT}`);