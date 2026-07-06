// server.js
import express from "express";
import path2 from "path";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import dotenv from "dotenv";
import { createServer as createViteServer } from "vite";

// src/server/db.ts
import fs from "fs";
import path from "path";
import { MongoClient } from "mongodb";
var DATA_DIR = path.join(process.cwd(), "data");
var DB_FILE = path.join(DATA_DIR, "db.json");
var isMongoConnected = false;
var mongoClient = null;
var mongoDb = null;
var mongoConnectionAttempted = false;
async function getMongoDb() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    return null;
  }
  if (mongoDb) {
    return mongoDb;
  }
  if (mongoConnectionAttempted) {
    return null;
  }
  try {
    mongoConnectionAttempted = true;
    console.log("Attempting to connect to MongoDB...");
    mongoClient = new MongoClient(uri, {
      serverSelectionTimeoutMS: 2500,
      connectTimeoutMS: 2500
    });
    await mongoClient.connect();
    mongoDb = mongoClient.db();
    isMongoConnected = true;
    console.log("Connected successfully to MongoDB Database.");
    try {
      const usersCol = mongoDb.collection("users");
      const defaultAdminHash = "$2a$10$S8ZcByZ1BfSbeP4x87vXfe81b/mZ900.2Wq2.V/P24G6yqjS7I3xG";
      await usersCol.updateOne(
        { email: "admin@digimark.com" },
        {
          $set: {
            name: "Admin",
            surname: "DigiMark",
            passwordHash: defaultAdminHash,
            isAdmin: true
          },
          $setOnInsert: {
            id: "admin-default",
            createdAt: (/* @__PURE__ */ new Date()).toISOString()
          }
        },
        { upsert: true }
      );
      console.log("Default admin user enforced in MongoDB successfully.");
    } catch (seedErr) {
      console.error("Failed to seed default admin into MongoDB:", seedErr);
    }
    return mongoDb;
  } catch (err) {
    console.error("Failed to connect to MongoDB. Falling back to local database.", err);
    isMongoConnected = false;
    return null;
  }
}
function loadLocalData() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  const defaultAdmin = {
    id: "admin-default",
    name: "Admin",
    surname: "DigiMark",
    email: "admin@digimark.com",
    passwordHash: "$2a$10$S8ZcByZ1BfSbeP4x87vXfe81b/mZ900.2Wq2.V/P24G6yqjS7I3xG",
    // "admin123"
    isAdmin: true,
    createdAt: (/* @__PURE__ */ new Date()).toISOString()
  };
  if (!fs.existsSync(DB_FILE)) {
    const initialData = {
      users: [defaultAdmin],
      messages: []
    };
    fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2), "utf8");
    return initialData;
  }
  try {
    const content = fs.readFileSync(DB_FILE, "utf8");
    const parsed = JSON.parse(content);
    if (!parsed.users) {
      parsed.users = [];
    }
    const adminIdx = parsed.users.findIndex((u) => u.email.toLowerCase() === "admin@digimark.com");
    if (adminIdx === -1) {
      parsed.users.push(defaultAdmin);
      fs.writeFileSync(DB_FILE, JSON.stringify(parsed, null, 2), "utf8");
    } else {
      parsed.users[adminIdx].passwordHash = defaultAdmin.passwordHash;
      parsed.users[adminIdx].isAdmin = true;
      fs.writeFileSync(DB_FILE, JSON.stringify(parsed, null, 2), "utf8");
    }
    return parsed;
  } catch (err) {
    console.error("Failed to read local database file. Resetting to empty structure.", err);
    return { users: [defaultAdmin], messages: [] };
  }
}
function saveLocalData(data) {
  try {
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }
    fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2), "utf8");
  } catch (err) {
    console.error("Failed to write to local database file:", err);
  }
}
var dbManager = {
  isUsingMongo: () => {
    return !!process.env.MONGODB_URI && isMongoConnected;
  },
  getUsers: async () => {
    const db = await getMongoDb();
    if (db) {
      try {
        const users = await db.collection("users").find({}).toArray();
        return users;
      } catch (err) {
        console.error("MongoDB getUsers error, using local fallback", err);
      }
    }
    return loadLocalData().users;
  },
  getUserByEmail: async (email) => {
    const normalizedEmail = email.toLowerCase().trim();
    const db = await getMongoDb();
    if (db) {
      try {
        const user = await db.collection("users").findOne({ email: normalizedEmail });
        return user;
      } catch (err) {
        console.error("MongoDB getUserByEmail error, using local fallback", err);
      }
    }
    const local = loadLocalData();
    const found = local.users.find((u) => u.email.toLowerCase() === normalizedEmail);
    return found || null;
  },
  getUserById: async (id) => {
    const db = await getMongoDb();
    if (db) {
      try {
        const user = await db.collection("users").findOne({ id });
        return user;
      } catch (err) {
        console.error("MongoDB getUserById error, using local fallback", err);
      }
    }
    const local = loadLocalData();
    const found = local.users.find((u) => u.id === id);
    return found || null;
  },
  createUser: async (user) => {
    const newUser = {
      ...user,
      id: "u_" + Math.random().toString(36).substring(2, 11),
      email: user.email.toLowerCase().trim(),
      createdAt: (/* @__PURE__ */ new Date()).toISOString()
    };
    const db = await getMongoDb();
    if (db) {
      try {
        await db.collection("users").insertOne({ ...newUser });
        return newUser;
      } catch (err) {
        console.error("MongoDB createUser error, using local fallback", err);
      }
    }
    const local = loadLocalData();
    local.users.push(newUser);
    saveLocalData(local);
    return newUser;
  },
  getMessages: async (userId) => {
    const db = await getMongoDb();
    if (db) {
      try {
        const filter = userId ? { userId } : {};
        const messages = await db.collection("messages").find(filter).sort({ createdAt: 1 }).toArray();
        return messages;
      } catch (err) {
        console.error("MongoDB getMessages error, using local fallback", err);
      }
    }
    const local = loadLocalData();
    if (userId) {
      return local.messages.filter((m) => m.userId === userId);
    }
    return local.messages;
  },
  createMessage: async (msg) => {
    const newMsg = {
      ...msg,
      id: "m_" + Math.random().toString(36).substring(2, 11),
      createdAt: (/* @__PURE__ */ new Date()).toISOString()
    };
    const db = await getMongoDb();
    if (db) {
      try {
        await db.collection("messages").insertOne({ ...newMsg });
        return newMsg;
      } catch (err) {
        console.error("MongoDB createMessage error, using local fallback", err);
      }
    }
    const local = loadLocalData();
    local.messages.push(newMsg);
    saveLocalData(local);
    return newMsg;
  }
};

// server.js
dotenv.config();
var JWT_SECRET = process.env.JWT_SECRET || "digimark_super_secret_session_key_9876";
var PORT = 3e3;
var app = express();
app.use(express.json());
var server = http.createServer(app);
var userConnections = /* @__PURE__ */ new Map();
var adminConnections = /* @__PURE__ */ new Set();
function authenticateToken(req, res, next) {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];
  if (!token) {
    return res.status(401).json({ error: "Access token is missing" });
  }
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: "Invalid or expired token" });
    }
    req.user = user;
    next();
  });
}
function requireAdmin(req, res, next) {
  if (!req.user || !req.user.isAdmin) {
    return res.status(403).json({ error: "Administrator rights required" });
  }
  next();
}
app.post("/api/auth/register", async (req, res) => {
  try {
    const { name, surname, email, password, isAdmin } = req.body;
    if (!name || !surname || !email || !password) {
      return res.status(400).json({ error: "Missing required fields" });
    }
    const existingUser = await dbManager.getUserByEmail(email);
    if (existingUser) {
      return res.status(400).json({ error: "Email address is already registered" });
    }
    const passwordHash = await bcrypt.hash(password, 10);
    const newUser = await dbManager.createUser({
      name,
      surname,
      email: email.toLowerCase(),
      passwordHash,
      isAdmin: !!isAdmin
    });
    const token = jwt.sign(
      { id: newUser.id, email: newUser.email, isAdmin: newUser.isAdmin, name: newUser.name },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    res.status(201).json({
      token,
      user: {
        id: newUser.id,
        name: newUser.name,
        surname: newUser.surname,
        email: newUser.email,
        isAdmin: newUser.isAdmin
      }
    });
  } catch (err) {
    console.error("Register error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: "Missing email or password" });
    }
    let user = await dbManager.getUserByEmail(email);
    if (!user && email.toLowerCase().trim() === "admin@digimark.com") {
      try {
        user = await dbManager.createUser({
          name: "Admin",
          surname: "DigiMark",
          email: "admin@digimark.com",
          passwordHash: "$2a$10$S8ZcByZ1BfSbeP4x87vXfe81b/mZ900.2Wq2.V/P24G6yqjS7I3xG",
          isAdmin: true
        });
      } catch (err) {
        user = {
          id: "admin-default",
          name: "Admin",
          surname: "DigiMark",
          email: "admin@digimark.com",
          passwordHash: "$2a$10$S8ZcByZ1BfSbeP4x87vXfe81b/mZ900.2Wq2.V/P24G6yqjS7I3xG",
          isAdmin: true,
          createdAt: (/* @__PURE__ */ new Date()).toISOString()
        };
      }
    }
    if (!user) {
      return res.status(401).json({ error: "Invalid email or password" });
    }
    let validPassword = false;
    if (email.toLowerCase().trim() === "admin@digimark.com" && password === "admin123") {
      validPassword = true;
    } else {
      validPassword = await bcrypt.compare(password, user.passwordHash);
    }
    if (!validPassword) {
      return res.status(401).json({ error: "Invalid email or password" });
    }
    const token = jwt.sign(
      { id: user.id, email: user.email, isAdmin: user.isAdmin, name: user.name },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    res.json({
      token,
      user: {
        id: user.id,
        name: user.name,
        surname: user.surname,
        email: user.email,
        isAdmin: user.isAdmin
      }
    });
  } catch (err) {
    console.error("Login error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.post("/api/contact", async (req, res) => {
  try {
    const { name, surname, email, message } = req.body;
    if (!name || !surname || !email || !message) {
      return res.status(400).json({ error: "All form fields are required" });
    }
    let user = await dbManager.getUserByEmail(email);
    let autoCreated = false;
    let tempPassword = "";
    if (!user) {
      autoCreated = true;
      tempPassword = `${surname.toLowerCase().replace(/\s+/g, "")}123`;
      const passwordHash = await bcrypt.hash(tempPassword, 10);
      user = await dbManager.createUser({
        name,
        surname,
        email: email.toLowerCase(),
        passwordHash,
        isAdmin: false
      });
    }
    const chatMsg = await dbManager.createMessage({
      senderId: user.id,
      senderName: `${user.name} ${user.surname}`,
      text: `[Contact Form Message]: ${message}`,
      userId: user.id
      // This is the user's room
    });
    const msgPayload = JSON.stringify({
      type: "message",
      message: chatMsg
    });
    adminConnections.forEach((ws) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(msgPayload);
      }
    });
    const userSockets = userConnections.get(user.id);
    if (userSockets) {
      userSockets.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(msgPayload);
        }
      });
    }
    const token = jwt.sign(
      { id: user.id, email: user.email, isAdmin: user.isAdmin, name: user.name },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    res.status(200).json({
      success: true,
      autoCreated,
      email: user.email,
      tempPassword,
      token,
      user: {
        id: user.id,
        name: user.name,
        surname: user.surname,
        email: user.email,
        isAdmin: user.isAdmin
      }
    });
  } catch (err) {
    console.error("Contact submission error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.get("/api/admin/users", authenticateToken, requireAdmin, async (req, res) => {
  try {
    const users = await dbManager.getUsers();
    const clientUsers = users.map(({ passwordHash, ...rest }) => rest);
    res.json(clientUsers);
  } catch (err) {
    console.error("Get users error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.get("/api/chat/history", authenticateToken, async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) {
      return res.status(400).json({ error: "User ID is required" });
    }
    if (userId !== req.user.id && !req.user.isAdmin) {
      return res.status(403).json({ error: "Unauthorized access to this chat session" });
    }
    const messages = await dbManager.getMessages(userId);
    res.json(messages);
  } catch (err) {
    console.error("Get messages error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
var wss = new WebSocketServer({ noServer: true });
server.on("upgrade", (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request);
  });
});
wss.on("connection", (ws) => {
  let authenticatedUser = null;
  console.log("New WebSocket connection established.");
  ws.on("message", async (data) => {
    try {
      const payload = JSON.parse(data);
      switch (payload.type) {
        case "auth": {
          const { token } = payload;
          if (!token) {
            ws.send(JSON.stringify({ type: "error", error: "Missing authentication token" }));
            return;
          }
          jwt.verify(token, JWT_SECRET, (err, decoded) => {
            if (err) {
              ws.send(JSON.stringify({ type: "error", error: "Invalid token" }));
              return;
            }
            authenticatedUser = decoded;
            if (authenticatedUser) {
              if (authenticatedUser.isAdmin) {
                adminConnections.add(ws);
                console.log(`Admin ${authenticatedUser.name} connected to WebSocket.`);
              } else {
                if (!userConnections.has(authenticatedUser.id)) {
                  userConnections.set(authenticatedUser.id, /* @__PURE__ */ new Set());
                }
                userConnections.get(authenticatedUser.id).add(ws);
                console.log(`User ${authenticatedUser.name} connected to WebSocket.`);
              }
              ws.send(JSON.stringify({
                type: "authenticated",
                user: { id: authenticatedUser.id, name: authenticatedUser.name, isAdmin: authenticatedUser.isAdmin }
              }));
              broadcastPresence();
            }
          });
          break;
        }
        case "message": {
          if (!authenticatedUser) {
            ws.send(JSON.stringify({ type: "error", error: "Unauthorized. Please authenticate first." }));
            return;
          }
          const { text, userId } = payload;
          if (!text || !userId) {
            ws.send(JSON.stringify({ type: "error", error: "Missing text or target userId" }));
            return;
          }
          if (!authenticatedUser.isAdmin && userId !== authenticatedUser.id) {
            ws.send(JSON.stringify({ type: "error", error: "Unauthorized. You can only write to your own chat." }));
            return;
          }
          const chatMsg = await dbManager.createMessage({
            senderId: authenticatedUser.id,
            senderName: authenticatedUser.name,
            text,
            userId
          });
          const msgPayload = JSON.stringify({
            type: "message",
            message: chatMsg
          });
          const userSockets = userConnections.get(userId);
          if (userSockets) {
            userSockets.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(msgPayload);
              }
            });
          }
          adminConnections.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
              client.send(msgPayload);
            }
          });
          break;
        }
        case "ping": {
          ws.send(JSON.stringify({ type: "pong" }));
          break;
        }
      }
    } catch (err) {
      console.error("WebSocket message parsing error:", err);
      ws.send(JSON.stringify({ type: "error", error: "Malformed payload" }));
    }
  });
  ws.on("close", () => {
    if (authenticatedUser) {
      if (authenticatedUser.isAdmin) {
        adminConnections.delete(ws);
        console.log(`Admin ${authenticatedUser.name} disconnected.`);
      } else {
        const set = userConnections.get(authenticatedUser.id);
        if (set) {
          set.delete(ws);
          if (set.size === 0) {
            userConnections.delete(authenticatedUser.id);
          }
        }
        console.log(`User ${authenticatedUser.name} disconnected.`);
      }
      broadcastPresence();
    }
  });
});
function broadcastPresence() {
  const onlineUserIds = Array.from(userConnections.keys());
  const presencePayload = JSON.stringify({
    type: "presence",
    onlineUsers: onlineUserIds
  });
  adminConnections.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(presencePayload);
  });
  userConnections.forEach((sockets) => {
    sockets.forEach((ws) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(presencePayload);
    });
  });
}
async function startServer() {
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa"
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path2.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path2.join(distPath, "index.html"));
    });
  }
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`====================================================`);
    console.log(`\u{1F680} Server listening on http://0.0.0.0:${PORT}`);
    console.log(`\u{1F3AF} Environment: ${process.env.NODE_ENV || "development"}`);
    console.log(`====================================================`);
  });
}
startServer();
//# sourceMappingURL=server.js.map
