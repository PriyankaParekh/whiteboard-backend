const mongoose = require("mongoose");
const { createClient } = require("redis");

let redisClient;

const connectDB = async () => {
  try {
    // 1. Connect MongoDB
    const mongoUri = process.env.MONGO_URI;
    if (!mongoUri) {
      throw new Error("MONGO_URI environment variable is not set");
    }
    await mongoose.connect(mongoUri);
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
