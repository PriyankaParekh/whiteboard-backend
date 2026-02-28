require("dotenv").config();
const express = require("express");
const app = express();
const port = 3001;
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { connectDB, getRedisClient } = require("./config/db");
const Room = require("./models/Room");
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
  },
});

connectDB();

// Track active rooms for safe persistence on shutdown
const activeRooms = new Set();

// Helper: Save Redis list for a room into MongoDB safely
async function saveRoomToMongo(roomId) {
  try {
    const redis = getRedisClient();
    if (!redis) {
      console.warn("Redis client unavailable, skipping save for", roomId);
      return;
    }

    const roomData = await redis.lRange(`room:${roomId}`, 0, -1);
    if (!roomData || roomData.length === 0) {
      return;
    }

    let parsedData;
    try {
      parsedData = roomData.map((item) => JSON.parse(item));
    } catch (err) {
      console.error("Failed to parse redis data for", roomId, err);
      return;
    }
    const now = new Date();
    const expiresAt = new Date(now.getTime() + 3 * 60 * 60 * 1000);
    await Room.findOneAndUpdate(
      { roomId },
      {
        $set: { elements: parsedData },
        $setOnInsert: {
          createdAt: now,
          expiresAt: expiresAt,
        },
      },
      { upsert: true, new: true },
    );
    console.log(`Room ${roomId} data saved to MongoDB!`);
  } catch (err) {
    console.error("Error saving room to MongoDB for", roomId, err);
  }
}

io.on("connection", (socket) => {
  console.log(`User Connected: ${socket.id}`);
  const redis = getRedisClient();

  // 1. JOIN ROOM LOGIC
  socket.on("join_room", async (roomId) => {
    socket.join(roomId);
    // Track active room
    activeRooms.add(roomId);

    const existingRoom = await Room.findOne({ roomId });
    if (existingRoom && existingRoom.expiresAt < new Date()) {
      socket.emit("room_expired"); // Tell client the room is expired
      socket.leave(roomId);
      return;
    }

    // Step A: Redis se data maango (Latest drawing)
    // LRANGE room:123 0 -1 (Matlab poora list dedo)
    let roomData = [];
    try {
      if (redis) roomData = await redis.lRange(`room:${roomId}`, 0, -1);
    } catch (err) {
      console.error("Redis lRange error for", roomId, err);
      roomData = [];
    }

    // Step B: Agar Redis khali hai, toh MongoDB check karo (Shayad server restart hua ho)
    if (roomData.length === 0) {
      const savedRoom = await Room.findOne({ roomId });
      if (savedRoom && savedRoom.elements.length > 0) {
        // Mongo se Redis mein wapas bhar do (Cache Warm-up)
        // Note: Mongo mein array hai, Redis mein list of strings chahiye
        const elements = savedRoom.elements.map((e) => JSON.stringify(e));
        try {
          if (redis) await redis.rPush(`room:${roomId}`, elements);
          roomData = elements;
        } catch (err) {
          console.error(
            "Redis rPush error during cache warm-up for",
            roomId,
            err,
          );
          // still use Mongo data to load to client
          roomData = elements;
        }
      }
    }

    // Step C: Data Frontend ko bhejo
    const parsedData = roomData.map((item) => JSON.parse(item));
    socket.emit("load_canvas", parsedData);
  });

  // 2. DRAWING LOGIC (Real-time)
  socket.on("draw_element", async (data) => {
    const { roomId, element } = data;

    const room = await Room.findOne({ roomId });
    if (room && room.expiresAt < new Date()) {
      socket.emit("room_expired");
      return;
    }

    // Step A: Doosron ko dikhao (Broadcast)
    socket.to(roomId).emit("receive_draw", element);

    // Step B: Redis mein store karo (RAM mein)
    // Key: room:123, Value: Element ka JSON string
    try {
      if (redis) await redis.rPush(`room:${roomId}`, JSON.stringify(element));
    } catch (err) {
      console.error("Redis rPush failed for", roomId, err);
    }
  });

  socket.on("delete_element", async (data) => {
    const { roomId, elementId } = data;
    socket.to(roomId).emit("element_deleted", { elementId });

    if (redis) {
      try {
        const list = await redis.lRange(`room:${roomId}`, 0, -1);
        const filtered = list.filter((item) => {
          try {
            const parsed = JSON.parse(item);
            return parsed.id !== elementId; // ← use `id`, not `elementId`
          } catch {
            return true; // keep item if parse fails
          }
        });
        await redis.del(`room:${roomId}`);
        if (filtered.length > 0) await redis.rPush(`room:${roomId}`, filtered);
      } catch (err) {
        console.error("Redis delete_element failed for", roomId, err);
      }
    }

    try {
      await Room.findOneAndUpdate(
        { roomId },
        { $pull: { elements: { id: elementId } } }, // ← pull by `id`
        { new: true },
      );
    } catch (err) {
      console.error("Mongo update failed on delete_element for", roomId, err);
    }
  });

  socket.on("leave_room", async (roomId) => {
    try {
      // stop tracking if no longer active
      activeRooms.delete(roomId);
      await saveRoomToMongo(roomId);
    } catch (err) {
      console.error("Error on leave_room for", roomId, err);
    }
  });

  // C. Disconnect
  socket.on("disconnect", () => {
    console.log("User Disconnected", socket.id);
  });
});

// Periodic autosave: persist active rooms every 30 seconds (non-blocking)
setInterval(() => {
  if (activeRooms.size === 0) return;
  for (const roomId of Array.from(activeRooms)) {
    // fire-and-forget, helper logs errors
    saveRoomToMongo(roomId);
  }
}, 30000);

// Graceful shutdown: attempt to persist active rooms before exit
async function gracefulShutdown() {
  console.log("Graceful shutdown: saving active rooms...");
  try {
    const saves = Array.from(activeRooms).map((r) => saveRoomToMongo(r));
    await Promise.allSettled(saves);
  } catch (err) {
    console.error("Error during graceful shutdown saves", err);
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
  // try to save and then exit
  gracefulShutdown();
});

// // --- BACKGROUND WORKER (AUTO-SAVE) ---
// // Har 10 second mein Redis ka data utha ke MongoDB mein save karo
// setInterval(async () => {
//   const redis = getRedisClient();
//   if (!redis) return;

//   // NOTE: Real production mein hum saare keys iterate karte hain (KEYS room:*)
//   // Lekin abhi simplicity ke liye maan lo humare paas active room IDs hain.
//   // (Aap active rooms ka Set maintain kar sakte ho)

//   // Example logic for a specific known room (Logic samjhane ke liye):
//   // Real app mein aapko activeRooms ka Set banana padega.

//   // --- THIS IS A SIMPLIFIED EXAMPLE FOR ONE ROOM LOGIC ---
//   // You need to track activeRoomIds in a Set variable globally
// }, 10000);

// // Better Logic for Save on Disconnect or specific Save Event to avoid complexity
// // Chalo abhi ke liye 'Save Button' approach ya 'Room Leave' approach best hai beginners ke liye.

server.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
