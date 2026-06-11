const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// ══════════ Binary WebSocket server للصوت من الـ ESP ══════════
// الـ ESP بيبعت صوت خام (binary) بدون base64/JSON — توفير ضخم في معالجة الـ ESP
// أول رسالة من الـ ESP لازم تكون نصية: "CODE:childSocketId" أو "REG:code:name"
// ⚡ noServer: true — نتعامل مع الـ upgrade يدوياً عشان مانتعارضش مع Socket.IO
const wss = new WebSocket.Server({ noServer: true });

// نوجّه فقط طلبات /esp-audio للـ ws — الباقي يسيبه لـ Socket.IO
server.on("upgrade", (request, socket, head) => {
  const { url } = request;
  if (url && url.startsWith("/esp-audio")) {
    wss.handleUpgrade(request, socket, head, (espWs) => {
      wss.emit("connection", espWs, request);
    });
  }
  // مهم: مانعملش socket.destroy() للباقي — Socket.IO بيتعامل معاه لوحده
});

// خريطة: roomCode -> espSocket (عشان نبعت أوامر start/stop للـ ESP)
const espSockets = {};

wss.on("connection", (espSocket) => {
  console.log("[ESP-AUDIO] ESP connected via binary WS");
  let espRoomCode = null;
  let espName = "ESP-S3";

  espSocket.on("message", (data, isBinary) => {
    if (!isBinary) {
      const text = data.toString();

      // تسجيل: REG:roomCode:name
      if (text.startsWith("REG:")) {
        const parts = text.split(":");
        espRoomCode = parts[1];
        espName = parts[2] || "ESP-S3";
        espSockets[espRoomCode] = espSocket;

        // سجّل الـ ESP كطفل في الغرفة
        const room = getOrCreateRoom(espRoomCode);
        const fakeId = "esp_" + espRoomCode;
        if (!room.children[espName]) {
          room.children[espName] = {
            socketId: fakeId, name: espName, location: null,
            battery: 100, online: true, appHidden: false, charging: true,
            isEsp: true
          };
        } else {
          room.children[espName].online = true;
          room.children[espName].socketId = fakeId;
        }

        // ألغِ أي مؤقت offline
        const timerKey = `${espRoomCode}::${espName}`;
        if (offlineTimers[timerKey]) {
          clearTimeout(offlineTimers[timerKey]);
          delete offlineTimers[timerKey];
        }

        // أبلغ الأب
        if (room.parent) {
          io.to(room.parent).emit("child_connected", {
            id: fakeId, name: espName, battery: 100,
            online: true, appHidden: false, charging: true
          });
        }
        console.log(`[ESP-AUDIO] registered "${espName}" in room ${espRoomCode}`);
      }
      return;
    }

    // binary = صوت خام
    if (!espRoomCode) return;
    const room = rooms[espRoomCode];
    if (!room || !room.parent) return;

    io.to(room.parent).emit("audio_chunk", {
      childId: "esp_" + espRoomCode,
      chunk: Buffer.from(data).toString("base64")
    });
  });

  espSocket.on("close", () => {
    console.log(`[ESP-AUDIO] ESP disconnected (room=${espRoomCode})`);
    if (espRoomCode) {
      delete espSockets[espRoomCode];
      const room = rooms[espRoomCode];
      if (room && room.children[espName]) {
        const timerKey = `${espRoomCode}::${espName}`;
        if (offlineTimers[timerKey]) clearTimeout(offlineTimers[timerKey]);
        offlineTimers[timerKey] = setTimeout(() => {
          const r = rooms[espRoomCode];
          if (r && r.children[espName]) {
            r.children[espName].online = false;
            if (r.parent) {
              io.to(r.parent).emit("child_offline", {
                id: "esp_" + espRoomCode, name: espName
              });
            }
          }
          delete offlineTimers[timerKey];
        }, OFFLINE_GRACE_MS);
      }
    }
  });

  espSocket.on("error", (e) => {
    console.log(`[ESP-AUDIO] error: ${e.message}`);
  });
});

// دالة مساعدة: ابعت أمر نصي للـ ESP لو متصل
function sendToEsp(roomCode, command) {
  const espSocket = espSockets[roomCode];
  if (espSocket && espSocket.readyState === WebSocket.OPEN) {
    espSocket.send(command);
    return true;
  }
  return false;
}

app.get("/ping", (req, res) => res.send("ok"));

const rooms = {};
const sleepTimers = {};

// ══════════ Grace period — مهلة قبل اعتبار الطفل offline ══════════
// تمنع الرمشة online/offline لما الـ ESP في وضع polling
const OFFLINE_GRACE_MS = 10000; // 10 ثواني
const offlineTimers = {};       // مفتاح: code::childName

function getOrCreateRoom(code) {
  if (!rooms[code]) rooms[code] = { parent: null, children: {}, lastHeartbeat: null };
  return rooms[code];
}

function cancelSleepTimer(code) {
  if (sleepTimers[code]) {
    clearTimeout(sleepTimers[code]);
    delete sleepTimers[code];
    console.log(`sleep timer cancelled for room: ${code}`);
  }
}

function scheduleSleepAllChildren(code) {
  if (sleepTimers[code]) clearTimeout(sleepTimers[code]);
  sleepTimers[code] = setTimeout(() => {
    const room = rooms[code];
    if (!room || room.parent) return;
    Object.values(room.children).forEach(child => {
      if (child.online) {
        io.to(child.socketId).emit("go_sleep");
        console.log(`go_sleep sent to child: ${child.name}`);
      }
    });
    delete sleepTimers[code];
  }, 5 * 60 * 1000);
  console.log(`sleep scheduled in 5 min for room: ${code}`);
}

// ══════════ Heartbeat checker ══════════
setInterval(() => {
  const now = Date.now();
  for (const code in rooms) {
    const room = rooms[code];
    if (room.parent && room.lastHeartbeat) {
      if (now - room.lastHeartbeat > 2 * 60 * 1000) {
        console.log(`heartbeat timeout for room: ${code}`);
        room.parent = null;
        room.lastHeartbeat = null;
        scheduleSleepAllChildren(code);
      }
    }
  }
}, 60 * 1000);

io.on("connection", (socket) => {
  console.log("connected:", socket.id);

  // ══════════ تسجيل الأب ══════════
  socket.on("register_parent", ({ code }) => {
    const room = getOrCreateRoom(code);
    room.parent = socket.id;
    room.lastHeartbeat = Date.now();
    socket.join(code);

    const childList = Object.values(room.children).map(c => ({
      id: c.socketId, name: c.name, location: c.location,
      battery: c.battery, online: c.online,
      appHidden: c.appHidden || false,
      charging: c.charging || false
    }));
    socket.emit("children_list", childList);
    cancelSleepTimer(code);
    console.log(`parent joined room: ${code}`);
  });

  // ══════════ Heartbeat ══════════
  socket.on("heartbeat", ({ code }) => {
    const room = rooms[code];
    if (room && room.parent === socket.id) {
      room.lastHeartbeat = Date.now();
    }
  });

  // ══════════ تسجيل الطفل ══════════
  socket.on("register_child", ({ code, name, battery, charging }) => {
    const room = getOrCreateRoom(code);

    // ألغِ أي مؤقت offline معلّق لهذا الطفل — رجع قبل ما تخلص المهلة
    const timerKey = `${code}::${name}`;
    if (offlineTimers[timerKey]) {
      clearTimeout(offlineTimers[timerKey]);
      delete offlineTimers[timerKey];
      console.log(`offline grace cancelled — "${name}" came back`);
    }

    const existing = room.children[name];
    if (existing) {
      const wasOffline = !existing.online;
      existing.socketId = socket.id;
      existing.online = true;
      existing.battery = battery || existing.battery;
      existing.charging = charging || false;
      socket.join(code);
      if (existing.appHidden) io.to(socket.id).emit("hide_app");
      io.to(socket.id).emit("go_sleep");
      // أبلغ الأب فقط لو كان فعلاً offline قبل كده (تجنّب رسائل زيادة)
      if (room.parent && wasOffline) {
        io.to(room.parent).emit("child_updated", {
          id: socket.id, name: existing.name, location: existing.location,
          battery: existing.battery, online: true,
          appHidden: existing.appHidden || false,
          charging: existing.charging || false
        });
      } else if (room.parent) {
        // تحديث صامت — نفس البيانات بدون تغيير حالة
        io.to(room.parent).emit("child_updated", {
          id: socket.id, name: existing.name, location: existing.location,
          battery: existing.battery, online: true,
          appHidden: existing.appHidden || false,
          charging: existing.charging || false
        });
      }
      console.log(`child "${name}" reconnected in room: ${code}`);
    } else {
      room.children[name] = {
        socketId: socket.id, name, location: null,
        battery: battery || null, online: true,
        appHidden: false, charging: charging || false
      };
      socket.join(code);
      io.to(socket.id).emit("go_sleep");
      if (room.parent) {
        io.to(room.parent).emit("child_connected", {
          id: socket.id, name, battery: battery || null,
          online: true, appHidden: false, charging: charging || false
        });
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

  // ══════════ البطارية + الشاحن ══════════
  socket.on("send_battery", ({ code, battery, charging }) => {
    const room = rooms[code];
    if (!room) return;
    const child = Object.values(room.children).find(c => c.socketId === socket.id);
    if (child) { child.battery = battery; child.charging = charging || false; }
    if (room.parent) {
      io.to(room.parent).emit("battery_update", { childId: socket.id, battery, charging: charging || false });
    }
  });

  // ══════════ الصوت ══════════
  socket.on("request_audio", ({ code, childId }) => {
    // لو الطفل ESP — ابعت الأمر عبر الـ binary WS
    if (childId && childId.startsWith("esp_")) {
      const roomCode = childId.substring(4);
      if (sendToEsp(roomCode, "start_audio")) {
        console.log(`[ESP-AUDIO] start_audio sent to room ${roomCode}`);
      }
      return;
    }
    io.to(childId).emit("start_audio");
    console.log(`audio requested for child: ${childId}`);
  });

  socket.on("stop_audio", ({ childId }) => {
    if (childId && childId.startsWith("esp_")) {
      const roomCode = childId.substring(4);
      sendToEsp(roomCode, "stop_audio");
      console.log(`[ESP-AUDIO] stop_audio sent to room ${roomCode}`);
      return;
    }
    io.to(childId).emit("stop_audio");
  });

  socket.on("audio_chunk", ({ code, chunk }) => {
    const room = rooms[code];
    if (!room || !room.parent) return;
    io.to(room.parent).emit("audio_chunk", { childId: socket.id, chunk });
  });

  // ══════════ صوت binary عبر Socket.IO — التحويل لـ base64 على السيرفر ══════════
  // الـ ESP بيبعت: emit("audio_bin", <Buffer خام>) — أول argument هو الكود كـ string
  // بنستقبل الـ binary ونحوّله base64 (على السيرفر القوي) ونبعته للأب بنفس صيغة audio_chunk
  socket.on("audio_bin", (codeBuf, audioBuf) => {
    // codeBuf ممكن يكون string (الكود) و audioBuf هو الـ binary
    let code, chunk;
    if (audioBuf === undefined) {
      // حالة: بعت الـ binary بس — نستخدم آخر كود مسجّل للـ socket
      code = socket.espCode;
      chunk = Buffer.from(codeBuf).toString("base64");
    } else {
      code = (typeof codeBuf === "string") ? codeBuf : Buffer.from(codeBuf).toString();
      socket.espCode = code;
      chunk = Buffer.from(audioBuf).toString("base64");
    }
    const room = rooms[code];
    if (!room) { console.log(`[audio_bin] no room for code=${code}`); return; }
    if (!room.parent) { console.log(`[audio_bin] no parent in room=${code}`); return; }
    io.to(room.parent).emit("audio_chunk", { childId: socket.id, chunk });
  });

  // ══════════ Wake/Sleep يدوي ══════════
  socket.on("wake_child", ({ childId }) => {
    io.to(childId).emit("wake_up");
    console.log(`wake_up sent to child: ${childId}`);
  });

  socket.on("sleep_child", ({ childId }) => {
    io.to(childId).emit("go_sleep");
  });

  // ══════════ Location مرة واحدة ══════════
  socket.on("get_location", ({ childId }) => {
    io.to(childId).emit("get_location");
    console.log(`get_location sent to child: ${childId}`);
  });

  // ══════════ إخفاء/إظهار التطبيق ══════════
  socket.on("hide_child_app", ({ childId }) => {
    for (const code in rooms) {
      const child = Object.values(rooms[code].children).find(c => c.socketId === childId);
      if (child) { child.appHidden = true; break; }
    }
    io.to(childId).emit("hide_app");
  });

  socket.on("show_child_app", ({ childId }) => {
    for (const code in rooms) {
      const child = Object.values(rooms[code].children).find(c => c.socketId === childId);
      if (child) { child.appHidden = false; break; }
    }
    io.to(childId).emit("show_app");
  });

  // ══════════ Ping ══════════
  socket.on("ping_server", () => socket.emit("pong_server"));

  // ══════════ Unregister parent يدوي ══════════
  socket.on("unregister_parent", ({ code }) => {
    const room = rooms[code];
    if (room) {
      room.parent = null;
      room.lastHeartbeat = null;
      scheduleSleepAllChildren(code);
      console.log(`parent manually unregistered: ${code}`);
    }
  });

  // ══════════ انقطاع الاتصال ══════════
  socket.on("disconnect", () => {
    for (const code in rooms) {
      const room = rooms[code];
      if (room.parent === socket.id) {
        room.parent = null;
        room.lastHeartbeat = null;
        scheduleSleepAllChildren(code);
        console.log(`parent disconnected from room: ${code}`);
      }
      const child = Object.values(room.children).find(c => c.socketId === socket.id);
      if (child) {
        const childName = child.name;
        const roomCode = code;
        const timerKey = `${roomCode}::${childName}`;

        // ══════════ Grace period — لا تقل offline فوراً ══════════
        // الـ ESP في وضع polling بيقطع وبيرجع كل ثانيتين — استنى 10 ثواني
        if (offlineTimers[timerKey]) clearTimeout(offlineTimers[timerKey]);
        offlineTimers[timerKey] = setTimeout(() => {
          const r = rooms[roomCode];
          if (!r) { delete offlineTimers[timerKey]; return; }
          const c = r.children[childName];
          // لو الطفل لسه على نفس الـ socket المقطوع — يبقى فعلاً offline
          if (c && c.socketId === socket.id) {
            c.online = false;
            if (r.parent) {
              io.to(r.parent).emit("child_offline", { id: socket.id, name: childName });
            }
            console.log(`child "${childName}" confirmed offline (grace expired)`);
          }
          delete offlineTimers[timerKey];
        }, OFFLINE_GRACE_MS);

        console.log(`child "${childName}" disconnected — grace period started (10s)`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
