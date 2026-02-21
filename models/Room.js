const mongoose = require("mongoose");

const roomSchema = new mongoose.Schema({
  roomId: { type: String, required: true, unique: true },
  elements: { type: Array, default: [] },
});

module.exports = mongoose.model("Room", roomSchema);
