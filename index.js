const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.get("/ping", (req, res) => res.send("ok"));

const rooms = {};

function getOrCreateRoom(code) {
  if (!rooms[code]) rooms[code] = { parent: null, children: {} };
  return rooms[code];
}

// ابعت wake لكل أطفال الروم
function wakeAllChildren(code) {
  const room = rooms[code];
  if (!room) return;
  Object.values(room.children).forEach(child => {
    if (child.online) {
      io.to(child.socketId).emit("wake_up");
      console.log(`wake_up sent to child: ${child.name}`);
    }
  });
}

// ابعت sleep لكل أطفال الروم
function sleepAllChildren(code) {
  const room = rooms[code];
  if (!room) return;
  Object.values(room.children).forEach(child => {
    if (child.online) {
      io.to(child.socketId).emit("go_sleep");
      console.log(`go_sleep sent to child: ${child.name}`);
    }
  });
}

io.on("connection", (socket) => {
  console.log("connected:", socket.id);

  // ══════════ تسجيل الأب ══════════
  socket.on("register_parent", ({ code }) => {
    const room = getOrCreateRoom(code);
    room.parent = socket.id;
    socket.join(code);

    // بعت قائمة الأطفال
    const childList = Object.values(room.children).map(c => ({
      id: c.socketId, name: c.name, location: c.location,
      battery: c.battery, online: c.online, appHidden: c.appHidden || false
    }));
    socket.emit("children_list", childList);

    // صحّي الأطفال لما الأب يفتح البرنامج
    wakeAllChildren(code);
    console.log(`parent joined room: ${code}`);
  });

  // ══════════ تسجيل الطفل ══════════
  socket.on("register_child", ({ code, name, battery }) => {
    const room = getOrCreateRoom(code);
    const existing = room.children[name];
    if (existing) {
      existing.socketId = socket.id;
      existing.online = true;
      existing.battery = battery || existing.battery;
      socket.join(code);
      if (existing.appHidden) io.to(socket.id).emit("hide_app");

      // لو الأب متصل — صحّي الطفل فوراً
      if (room.parent) {
        io.to(socket.id).emit("wake_up");
        io.to(room.parent).emit("child_updated", {
          id: socket.id, name: existing.name, location: existing.location,
          battery: existing.battery, online: true, appHidden: existing.appHidden || false
        });
      } else {
        // الأب مش متصل — نام
        io.to(socket.id).emit("go_sleep");
      }
      console.log(`child "${name}" reconnected in room: ${code}`);
    } else {
      room.children[name] = {
        socketId: socket.id, name, location: null,
        battery: battery || null, online: true, appHidden: false
      };
      socket.join(code);

      if (room.parent) {
        // الأب متصل — صحّي الطفل
        io.to(socket.id).emit("wake_up");
        io.to(room.parent).emit("child_connected", {
          id: socket.id, name, battery: battery || null, online: true, appHidden: false
        });
      } else {
        // الأب مش متصل — نام
        io.to(socket.id).emit("go_sleep");
      }
      console.log(`child "${name}" joined room: ${code}`);
    }
  });

  // ══════════ الموقع ══════════
  socket.on("send_location", ({ code, location }) => {
    const room = rooms[code];
    if (!room) return;
    const child = Object.values(room.children).find(c => c.socketId === socket.id);
    if (child) child.location = location;
    if (room.parent) io.to(room.parent).emit("location_update", { childId: socket.id, location });
  });

  // ══════════ البطارية ══════════
  socket.on("send_battery", ({ code, battery }) => {
    const room = rooms[code];
    if (!room) return;
    const child = Object.values(room.children).find(c => c.socketId === socket.id);
    if (child) child.battery = battery;
    if (room.parent) io.to(room.parent).emit("battery_update", { childId: socket.id, battery });
  });

  // ══════════ الصوت ══════════
  socket.on("request_audio", ({ code, childId }) => {
    io.to(childId).emit("start_audio");
    console.log(`audio requested for child: ${childId}`);
  });

  socket.on("stop_audio", ({ childId }) => {
    io.to(childId).emit("stop_audio");
  });

  socket.on("audio_chunk", ({ code, chunk }) => {
    const room = rooms[code];
    if (!room || !room.parent) return;
    io.to(room.parent).emit("audio_chunk", { childId: socket.id, chunk });
  });

  // ══════════ Wake/Sleep يدوي من الأب ══════════
  socket.on("wake_child", ({ childId }) => {
    io.to(childId).emit("wake_up");
    console.log(`manual wake sent to: ${childId}`);
  });

  socket.on("sleep_child", ({ childId }) => {
    io.to(childId).emit("go_sleep");
    console.log(`manual sleep sent to: ${childId}`);
  });

  // ══════════ إخفاء/إظهار التطبيق ══════════
  socket.on("hide_child_app", ({ childId }) => {
    for (const code in rooms) {
      const child = Object.values(rooms[code].children).find(c => c.socketId === childId);
      if (child) { child.appHidden = true; break; }
    }
    io.to(childId).emit("hide_app");
    console.log(`hide app: ${childId}`);
  });

  socket.on("show_child_app", ({ childId }) => {
    for (const code in rooms) {
      const child = Object.values(rooms[code].children).find(c => c.socketId === childId);
      if (child) { child.appHidden = false; break; }
    }
    io.to(childId).emit("show_app");
    console.log(`show app: ${childId}`);
  });

  // ══════════ Ping ══════════
  socket.on("ping_server", () => socket.emit("pong_server"));

  // ══════════ انقطاع الاتصال ══════════
  socket.on("disconnect", () => {
    for (const code in rooms) {
      const room = rooms[code];

      // لو الأب انقطع — نوّم الأطفال
      if (room.parent === socket.id) {
        room.parent = null;
        sleepAllChildren(code);
        console.log(`parent disconnected from room: ${code} — children sleeping`);
      }

      const child = Object.values(room.children).find(c => c.socketId === socket.id);
      if (child) {
        child.online = false;
        if (room.parent) io.to(room.parent).emit("child_offline", { id: socket.id, name: child.name });
        console.log(`child "${child.name}" went offline`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));