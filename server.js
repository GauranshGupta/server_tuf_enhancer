const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const wss = new WebSocketServer({ port: PORT });

// roomId -> Map(username -> { currentProblem, sockets: Set(ws) })
const rooms = new Map();

function generateRoomId() {
  let roomId;
  do {
    // Generates a readable 6-character code like "TUF-9B2X"
    const randomHex = crypto.randomBytes(3).toString("hex").toUpperCase();
    roomId = `TUF-${randomHex}`;
  } while (rooms.has(roomId));
  return roomId;
}

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

  const message = JSON.stringify({ 
    type: "ROOM_PRESENCE", 
    roomId,
    members 
  });

  for (const data of room.values()) {
    for (const client of data.sockets) {
      if (client.readyState === 1) { // OPEN
        client.send(message);
      }
    }
  }
}

wss.on("connection", (ws) => {
  let userRoom = null;
  let userName = null;

  ws.on("error", console.error);
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    try {
      const data = JSON.parse(raw);

      // 1. CREATE NEW ROOM
      if (data.type === "CREATE_ROOM") {
        const newRoomId = generateRoomId();
        rooms.set(newRoomId, new Map());

        ws.send(JSON.stringify({
          type: "ROOM_CREATED",
          roomId: newRoomId
        }));
        console.log(`Created room: ${newRoomId}`);
        return;
      }

      // 2. JOIN ROOM
      if (data.type === "JOIN_ROOM") {
        const { roomId, username } = data;
        const normalizedRoomId = (roomId || "").trim().toUpperCase();

        if (!rooms.has(normalizedRoomId)) {
          // If room doesn't exist, notify client
          ws.send(JSON.stringify({
            type: "ROOM_ERROR",
            message: `Room "${normalizedRoomId}" does not exist. Check the ID or create a new room.`
          }));
          return;
        }

        userRoom = normalizedRoomId;
        userName = (username || "Anonymous").trim();

        const room = rooms.get(userRoom);

        if (!room.has(userName)) {
          room.set(userName, {
            currentProblem: "Viewing sheet",
            sockets: new Set()
          });
        }

        room.get(userName).sockets.add(ws);

        ws.send(JSON.stringify({
          type: "JOIN_SUCCESS",
          roomId: userRoom,
          username: userName
        }));

        console.log(`🟢 ${userName} joined room: ${userRoom}`);
        broadcastRoomPresence(userRoom);
      }

      // 3. SET PROBLEM
      if (data.type === "SET_PROBLEM" && userRoom && userName && rooms.has(userRoom)) {
        const room = rooms.get(userRoom);
        if (room.has(userName)) {
          room.get(userName).currentProblem = data.problemName;
          broadcastRoomPresence(userRoom);
        }
      }

      // 4. EXPLICIT LEAVE ROOM
      if (data.type === "LEAVE_ROOM" && userRoom && userName && rooms.has(userRoom)) {
        cleanupUser(userRoom, userName, ws);
        userRoom = null;
        userName = null;
      }
    } catch (err) {
      console.error("Message parse error:", err);
    }
  });

  function cleanupUser(roomId, user, socket) {
    if (!rooms.has(roomId)) return;
    const room = rooms.get(roomId);

    if (room.has(user)) {
      const userData = room.get(user);
      userData.sockets.delete(socket);

      if (userData.sockets.size === 0) {
        room.delete(user);
        console.log(`🔴 ${user} left room: ${roomId}`);
      }
    }

    if (room.size === 0) {
      rooms.delete(roomId);
      console.log(`🗑️ Room deleted (empty): ${roomId}`);
    } else {
      broadcastRoomPresence(roomId);
    }
  }

  ws.on("close", () => {
    if (userRoom && userName) {
      cleanupUser(userRoom, userName, ws);
    }
  });
});

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