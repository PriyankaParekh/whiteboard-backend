const mongoose = require("mongoose");

const roomSchema = new mongoose.Schema({
  roomId: { type: String, required: true, unique: true },
  elements: { type: Array, default: [] },
  createdAt: { type: Date, default: Date.now, immutable: true },
  expiresAt: {
    type: Date,
    default: () => new Date(Date.now() + 3 * 60 * 60 * 1000),
    immutable: true,
  },
});

// MongoDB TTL index — auto-deletes documents 24 hours after expiry (1 day grace period)
// This runs automatically in the background via MongoDB's TTL monitor
roomSchema.index(
  { expiresAt: 1 },
  { expireAfterSeconds: 24 * 60 * 60 }, // delete 1 day AFTER expiresAt
);

module.exports = mongoose.model("Room", roomSchema);
