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

const activeRooms = new Set();

// ─── Per-room debounce timers for MongoDB saves ────────────────────────────
const mongoSaveTimers = new Map();

function scheduleSaveToMongo(roomId) {
  if (mongoSaveTimers.has(roomId)) clearTimeout(mongoSaveTimers.get(roomId));
  const timer = setTimeout(async () => {
    mongoSaveTimers.delete(roomId);
    await saveRoomToMongo(roomId);
  }, 2000);
  mongoSaveTimers.set(roomId, timer);
}

// ─── Per-room mutex to prevent concurrent Redis read-modify-write races ────
// Without this, two simultaneous delete_element events read the SAME stale
// list, each remove their own element, then both write back — the second
// write restores the element the first write deleted.
const roomLocks = new Map();

async function withRoomLock(roomId, fn) {
  // Chain operations so they execute one at a time per room
  const prev = roomLocks.get(roomId) || Promise.resolve();
  const next = prev.then(fn).catch((err) => {
    console.error(`Lock fn error for room ${roomId}:`, err);
  });
  roomLocks.set(roomId, next);
  await next;
}

// ─── Save Redis → MongoDB ──────────────────────────────────────────────────
async function saveRoomToMongo(roomId) {
  try {
    const redis = getRedisClient();
    if (!redis) return;

    const roomData = await redis.lRange(`room:${roomId}`, 0, -1);
    if (!roomData || roomData.length === 0) return;

    const parsedData = roomData
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch (_) {
          return null;
        }
      })
      .filter(Boolean);

    const now = new Date();
    const expiresAt = new Date(now.getTime() + 3 * 60 * 60 * 1000);

    await Room.findOneAndUpdate(
      { roomId },
      {
        $set: { elements: parsedData },
        $setOnInsert: { createdAt: now, expiresAt },
      },
      { upsert: true, new: true },
    );
    console.log(
      `✅ Room ${roomId} saved to MongoDB (${parsedData.length} elements)`,
    );
  } catch (err) {
    console.error("Error saving room to MongoDB for", roomId, err);
  }
}

// ─── Upsert one element in Redis (no duplicates) ──────────────────────────
// Must be called inside withRoomLock to be safe.
async function upsertElementInRedis(roomId, element) {
  const redis = getRedisClient();
  if (!redis) return;
  const key = `room:${roomId}`;
  const list = await redis.lRange(key, 0, -1);
  let replaced = false;
  for (let i = 0; i < list.length; i++) {
    try {
      if (JSON.parse(list[i]).id === element.id) {
        await redis.lSet(key, i, JSON.stringify(element));
        replaced = true;
        break;
      }
    } catch (_) {}
  }
  if (!replaced) {
    await redis.rPush(key, JSON.stringify(element));
  }
}

// ─── Delete multiple elements from Redis atomically ────────────────────────
// Takes a Set of IDs to remove. One read-filter-write operation so no race.
// Must be called inside withRoomLock to be safe.
async function deleteElementsFromRedis(roomId, idSet) {
  const redis = getRedisClient();
  if (!redis) return;
  const key = `room:${roomId}`;
  const list = await redis.lRange(key, 0, -1);
  const filtered = list.filter((item) => {
    try {
      return !idSet.has(JSON.parse(item).id);
    } catch (_) {
      return true;
    }
  });
  await redis.del(key);
  if (filtered.length > 0) await redis.rPush(key, filtered);
}

// ─── Socket handlers ───────────────────────────────────────────────────────
io.on("connection", (socket) => {
  console.log(`User Connected: ${socket.id}`);

  // 1. JOIN ROOM
  socket.on("join_room", async (roomId) => {
    socket.join(roomId);
    activeRooms.add(roomId);
    const redis = getRedisClient();

    const existingRoom = await Room.findOne({ roomId });
    if (existingRoom && existingRoom.expiresAt < new Date()) {
      socket.emit("room_expired");
      socket.leave(roomId);
      return;
    }

    let roomData = [];
    try {
      if (redis) roomData = await redis.lRange(`room:${roomId}`, 0, -1);
    } catch (err) {
      console.error("Redis lRange error for", roomId, err);
    }

    if (
      roomData.length === 0 &&
      existingRoom &&
      existingRoom.elements.length > 0
    ) {
      const elements = existingRoom.elements.map((e) => JSON.stringify(e));
      try {
        if (redis && elements.length > 0)
          await redis.rPush(`room:${roomId}`, elements);
        roomData = elements;
      } catch (err) {
        roomData = elements;
      }
    }

    const parsedData = roomData
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch (_) {
          return null;
        }
      })
      .filter(Boolean);

    socket.emit("load_canvas", parsedData);
  });

  // 2. DRAW / UPDATE ELEMENT
  socket.on("draw_element", async ({ roomId, element }) => {
    const room = await Room.findOne({ roomId });
    if (room && room.expiresAt < new Date()) {
      socket.emit("room_expired");
      return;
    }

    socket.to(roomId).emit("receive_draw", element);

    // Lock ensures upsert is atomic per room
    await withRoomLock(roomId, () => upsertElementInRedis(roomId, element));

    // Debounced MongoDB save
    scheduleSaveToMongo(roomId);
  });

  // 3. DELETE SINGLE ELEMENT
  socket.on("delete_element", async ({ roomId, elementId }) => {
    socket.to(roomId).emit("element_deleted", { elementId });

    // Use the batch delete with a single-item Set — goes through the lock
    await withRoomLock(roomId, () =>
      deleteElementsFromRedis(roomId, new Set([elementId])),
    );

    // Save to MongoDB immediately for deletions (don't wait for debounce)
    try {
      await Room.findOneAndUpdate(
        { roomId },
        { $pull: { elements: { id: elementId } } },
        { new: true },
      );
      console.log(`🗑️  Element ${elementId} deleted from MongoDB`);
    } catch (err) {
      console.error("Mongo delete_element failed for", roomId, err);
    }
  });

  // 4. DELETE MULTIPLE ELEMENTS — new batch event (add this to frontend too)
  // Frontend should emit this instead of looping delete_element when
  // deleting multiple elements at once (e.g. deleteSelected).
  socket.on("delete_elements", async ({ roomId, elementIds }) => {
    if (!elementIds || elementIds.length === 0) return;

    const idSet = new Set(elementIds);

    // Broadcast to other clients
    socket.to(roomId).emit("elements_deleted", { elementIds });

    // ONE atomic Redis operation removes all IDs at once — no race condition
    await withRoomLock(roomId, () => deleteElementsFromRedis(roomId, idSet));

    // ONE MongoDB operation removes all at once
    try {
      await Room.findOneAndUpdate(
        { roomId },
        { $pull: { elements: { id: { $in: elementIds } } } },
        { new: true },
      );
      console.log(
        `🗑️  Elements [${elementIds.join(", ")}] deleted from MongoDB`,
      );
    } catch (err) {
      console.error("Mongo delete_elements failed for", roomId, err);
    }
  });

  // 5. LEAVE ROOM
  socket.on("leave_room", async (roomId) => {
    try {
      if (mongoSaveTimers.has(roomId)) {
        clearTimeout(mongoSaveTimers.get(roomId));
        mongoSaveTimers.delete(roomId);
      }
      await saveRoomToMongo(roomId);
      activeRooms.delete(roomId);
    } catch (err) {
      console.error("Error on leave_room for", roomId, err);
    }
  });

  // 6. DISCONNECTING — fires while socket.rooms still has room memberships
  socket.on("disconnecting", () => {
    for (const roomId of socket.rooms) {
      if (roomId === socket.id) continue;
      scheduleSaveToMongo(roomId);
    }
  });

  socket.on("disconnect", () => {
    console.log("User Disconnected", socket.id);
  });
});

// Periodic autosave every 30s as safety net
setInterval(() => {
  for (const roomId of Array.from(activeRooms)) {
    saveRoomToMongo(roomId);
  }
}, 30000);

async function gracefulShutdown() {
  console.log("Graceful shutdown: saving active rooms...");
  for (const [roomId, timer] of mongoSaveTimers) {
    clearTimeout(timer);
    await saveRoomToMongo(roomId);
  }
  try {
    await Promise.allSettled(
      Array.from(activeRooms).map((r) => saveRoomToMongo(r)),
    );
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
process.on("unhandledRejection", (reason) =>
  console.error("Unhandled Rejection:", reason),
);
process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
  gracefulShutdown();
});

server.listen(port, () => console.log(`Server is running on port ${port}`));
