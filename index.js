const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// rooms[code] = { parent: socketId, children: { name: {socketId, name, location, battery, online} } }
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

    // بعت قائمة الأطفال الموجودين
    const childList = Object.values(room.children).map(c => ({
      id: c.socketId,
      name: c.name,
      location: c.location,
      battery: c.battery,
      online: c.online
    }));
    socket.emit("children_list", childList);
    console.log(`parent joined room: ${code}`);
  });

  // ══════════ تسجيل الطفل — بالاسم كمفتاح منع التكرار ══════════
  socket.on("register_child", ({ code, name, battery }) => {
    const room = getOrCreateRoom(code);

    // لو الطفل ده موجود قبل كده بنفس الاسم — نحدّث الـ socketId بس
    const existing = room.children[name];
    if (existing) {
      // الطفل القديم انقطع وعاد — نحدّث الـ socketId
      const oldSocketId = existing.socketId;
      existing.socketId = socket.id;
      existing.online = true;
      existing.battery = battery || existing.battery;

      socket.join(code);

      // أبلغ الأب إن الطفل ده عاد أونلاين
      if (room.parent) {
        io.to(room.parent).emit("child_updated", {
          id: socket.id,
          name: existing.name,
          location: existing.location,
          battery: existing.battery,
          online: true
        });
      }
      console.log(`child "${name}" reconnected in room: ${code}`);
    } else {
      // طفل جديد
      room.children[name] = {
        socketId: socket.id,
        name,
        location: null,
        battery: battery || null,
        online: true
      };
      socket.join(code);

      if (room.parent) {
        io.to(room.parent).emit("child_connected", {
          id: socket.id,
          name,
          battery: battery || null,
          online: true
        });
      }
      console.log(`child "${name}" joined room: ${code}`);
    }
  });

  // ══════════ الموقع ══════════
  socket.on("send_location", ({ code, location }) => {
    const room = rooms[code];
    if (!room) return;

    // ابحث عن الطفل بالـ socketId
    const child = Object.values(room.children).find(c => c.socketId === socket.id);
    if (child) {
      child.location = location;
    }

    if (room.parent) {
      io.to(room.parent).emit("location_update", { childId: socket.id, location });
    }
  });

  // ══════════ البطارية ══════════
  socket.on("send_battery", ({ code, battery }) => {
    const room = rooms[code];
    if (!room) return;

    const child = Object.values(room.children).find(c => c.socketId === socket.id);
    if (child) {
      child.battery = battery;
    }

    if (room.parent) {
      io.to(room.parent).emit("battery_update", { childId: socket.id, battery });
    }
  });

  // ══════════ الصوت ══════════
  socket.on("request_audio", ({ code, childId }) => {
    console.log(`audio requested for child: ${childId}`);
    io.to(childId).emit("start_audio");
  });

  socket.on("stop_audio", ({ childId }) => {
    console.log(`audio stopped for child: ${childId}`);
    io.to(childId).emit("stop_audio");
  });

  socket.on("audio_chunk", ({ code, chunk }) => {
    const room = rooms[code];
    if (!room || !room.parent) return;
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

      // ابحث عن الطفل بالـ socketId
      const child = Object.values(room.children).find(c => c.socketId === socket.id);
      if (child) {
        child.online = false;
        // مش بنحذفه — بس بنقول إنه أوفلاين
        if (room.parent) {
          io.to(room.parent).emit("child_offline", { id: socket.id, name: child.name });
        }
        console.log(`child "${child.name}" went offline`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));