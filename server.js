const express = require('express');
const app = express();
const port = 3001;
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "http://localhost:3000",
        methods: ["GET", "POST"],
    },
});

io.on("connection", (socket) => {
    console.log(`User Connected: ${socket.id}`);

    // A. Room Join karna (Jaise alag-alag Meeting Rooms)
    socket.on("join_room", (roomId) => {
        socket.join(roomId);
        console.log(`User ${socket.id} joined room: ${roomId}`);
    });

    // B. Drawing Data receive karna aur baaki logo ko bhejna
    socket.on("draw_stroke", (data) => {
        // data = { x: 10, y: 20, color: "red", roomId: "123" }

        // Sirf uss room ke logon ko bhejo, bhejnewale ko chod ke
        socket.to(data.roomId).emit("receive_stroke", data);
    });

    // C. Disconnect
    socket.on("disconnect", () => {
        console.log("User Disconnected", socket.id);
    });
});

server.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});