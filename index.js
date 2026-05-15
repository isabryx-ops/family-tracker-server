const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// rooms[code] = { parent: socketId, children: { socketId: {name, location} } }
const rooms = {};

function getOrCreateRoom(code) {
  if (!rooms[code]) rooms[code] = { parent: null, children: {} };
  return rooms[code];
}

io.on("connection", (socket) => {
  console.log("connected:", socket.id);

  // ══════════ تسجيل الأب ══════════
  socket.on("register_parent", ({ code }) => {
    const room = getOrCreateRoom(code);
    room.parent = socket.id;
    socket.join(code);
    socket.emit("children_list", Object.values(room.children));
    console.log(`parent joined room: ${code}`);
  });

  // ══════════ تسجيل الطفل ══════════
  socket.on("register_child", ({ code, name }) => {
    const room = getOrCreateRoom(code);
    room.children[socket.id] = { id: socket.id, name, location: null };
    socket.join(code);
    if (room.parent) {
      io.to(room.parent).emit("child_connected", { id: socket.id, name });
    }
    console.log(`child "${name}" joined room: ${code}`);
  });

  // ══════════ الموقع ══════════
  socket.on("send_location", ({ code, location }) => {
    const room = rooms[code];
    if (!room) return;
    if (room.children[socket.id]) {
      room.children[socket.id].location = location;
    }
    if (room.parent) {
      io.to(room.parent).emit("location_update", { childId: socket.id, location });
    }
  });

  // ══════════ الصوت ══════════

  // الأب يطلب الاستماع
  socket.on("request_audio", ({ code, childId }) => {
    console.log(`audio requested for child: ${childId}`);
    io.to(childId).emit("start_audio");
  });

  // الأب يوقف الاستماع
  socket.on("stop_audio", ({ childId }) => {
    console.log(`audio stopped for child: ${childId}`);
    io.to(childId).emit("stop_audio");
  });

  // الطفل يبعت chunk صوت
  socket.on("audio_chunk", ({ code, chunk }) => {
    const room = rooms[code];
    if (!room || !room.parent) return;
    console.log(`audio chunk received from child, sending to parent`);
    io.to(room.parent).emit("audio_chunk", { childId: socket.id, chunk });
  });

  // ══════════ انقطاع الاتصال ══════════
  socket.on("disconnect", () => {
    for (const code in rooms) {
      const room = rooms[code];
      if (room.parent === socket.id) {
        room.parent = null;
        console.log(`parent disconnected from room: ${code}`);
      }
      if (room.children[socket.id]) {
        const name = room.children[socket.id].name;
        delete room.children[socket.id];
        if (room.parent) {
          io.to(room.parent).emit("child_disconnected", { id: socket.id, name });
        }
        console.log(`child "${name}" disconnected`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
