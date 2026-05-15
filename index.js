const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

// rooms[familyCode] = { parent: socketId, children: { socketId: {name, location} } }
const rooms = {};

function getOrCreateRoom(code) {
  if (!rooms[code]) rooms[code] = { parent: null, children: {} };
  return rooms[code];
}

io.on("connection", (socket) => {
  console.log("connected:", socket.id);

  // الأب يسجل نفسه
  socket.on("register_parent", ({ code }) => {
    const room = getOrCreateRoom(code);
    room.parent = socket.id;
    socket.join(code);
    console.log(`parent registered in room: ${code}`);

    // يبعت قائمة الأطفال الموجودين
    socket.emit("children_list", Object.values(room.children));
  });

  // الطفل يسجل نفسه
  socket.on("register_child", ({ code, name }) => {
    const room = getOrCreateRoom(code);
    room.children[socket.id] = { id: socket.id, name, location: null };
    socket.join(code);
    console.log(`child "${name}" joined room: ${code}`);

    // يبلغ الأب إن طفل جديد اتصل
    if (room.parent) {
      io.to(room.parent).emit("child_connected", { id: socket.id, name });
    }
  });

  // الطفل يبعت الموقع
  socket.on("send_location", ({ code, location }) => {
    const room = rooms[code];
    if (!room) return;
    if (room.children[socket.id]) {
      room.children[socket.id].location = location;
    }
    // يبعت للأب
    if (room.parent) {
      io.to(room.parent).emit("location_update", {
        childId: socket.id,
        location
      });
    }
  });

  // الأب يطلب الاستماع لطفل معين
  socket.on("request_audio", ({ code, childId }) => {
    io.to(childId).emit("start_audio");
    console.log(`audio requested for child: ${childId}`);
  });

  // الأب يوقف الاستماع
  socket.on("stop_audio", ({ childId }) => {
    io.to(childId).emit("stop_audio");
  });

  // الطفل يبعت بيانات الصوت
  socket.on("audio_chunk", ({ code, chunk }) => {
    const room = rooms[code];
    if (!room || !room.parent) return;
    io.to(room.parent).emit("audio_chunk", {
      childId: socket.id,
      chunk
    });
  });

  // عند انقطاع الاتصال
  socket.on("disconnect", () => {
    for (const code in rooms) {
      const room = rooms[code];
      // لو الأب انقطع
      if (room.parent === socket.id) {
        room.parent = null;
        console.log(`parent disconnected from room: ${code}`);
      }
      // لو طفل انقطع
      if (room.children[socket.id]) {
        const name = room.children[socket.id].name;
        delete room.children[socket.id];
        if (room.parent) {
          io.to(room.parent).emit("child_disconnected", { id: socket.id, name });
        }
        console.log(`child "${name}" disconnected from room: ${code}`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
