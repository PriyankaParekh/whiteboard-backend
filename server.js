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

io.on("connection", (socket) => {
  console.log(`User Connected: ${socket.id}`);
  const redis = getRedisClient();

  // 1. JOIN ROOM LOGIC
  socket.on("join_room", async (roomId) => {
    socket.join(roomId);

    // Step A: Redis se data maango (Latest drawing)
    // LRANGE room:123 0 -1 (Matlab poora list dedo)
    let roomData = await redis.lRange(`room:${roomId}`, 0, -1);

    // Step B: Agar Redis khali hai, toh MongoDB check karo (Shayad server restart hua ho)
    if (roomData.length === 0) {
      const savedRoom = await Room.findOne({ roomId });
      if (savedRoom && savedRoom.elements.length > 0) {
        // Mongo se Redis mein wapas bhar do (Cache Warm-up)
        // Note: Mongo mein array hai, Redis mein list of strings chahiye
        const elements = savedRoom.elements.map((e) => JSON.stringify(e));
        await redis.rPush(`room:${roomId}`, elements);
        roomData = elements;
      }
    }

    // Step C: Data Frontend ko bhejo
    const parsedData = roomData.map((item) => JSON.parse(item));
    socket.emit("load_canvas", parsedData);
  });

  // 2. DRAWING LOGIC (Real-time)
  socket.on("draw_element", async (data) => {
    const { roomId, element } = data;

    // Step A: Doosron ko dikhao (Broadcast)
    socket.to(roomId).emit("receive_draw", element);

    // Step B: Redis mein store karo (RAM mein)
    // Key: room:123, Value: Element ka JSON string
    await redis.rPush(`room:${roomId}`, JSON.stringify(element));
  });

  socket.on("leave_room", async (roomId) => {
    const redis = getRedisClient();

    // 1. Redis se sara data nikalo
    const roomData = await redis.lRange(`room:${roomId}`, 0, -1);

    if (roomData.length > 0) {
      const parsedData = roomData.map((item) => JSON.parse(item));

      // 2. MongoDB mein update/upsert karo
      await Room.findOneAndUpdate(
        { roomId },
        { elements: parsedData },
        { upsert: true, new: true },
      );
      console.log(`Room ${roomId} data saved to MongoDB!`);
    }
  });

  // C. Disconnect
  socket.on("disconnect", () => {
    console.log("User Disconnected", socket.id);
  });
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
