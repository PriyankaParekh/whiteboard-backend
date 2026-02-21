const mongoose = require("mongoose");
const { createClient } = require("redis");

let redisClient;

const connectDB = async () => {
  try {
    // 1. Connect MongoDB
    await mongoose.connect(
      process.env.MONGO_URI || "mongodb://localhost:27017/whiteboard_db",
    );
    console.log("✅ MongoDB Connected");

    // 2. Connect Redis
    redisClient = createClient({
      url: process.env.REDIS_URL || "redis://localhost:6379",
    });

    redisClient.on("error", (err) => console.log("Redis Error:", err));
    await redisClient.connect();
    console.log("✅ Redis Connected");
  } catch (err) {
    console.error("❌ Database Connection Failed:", err);
    process.exit(1);
  }
};

const getRedisClient = () => redisClient;

module.exports = { connectDB, getRedisClient };
