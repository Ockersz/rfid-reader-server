require("dotenv").config();
const https = require("https");
const fs = require("fs");
const DatabaseManager = require("./databaseManager");
const UDPServer = require("./udpServer");
const axios = require("axios");

let dbManager, serverDbManager, httpServer, udpServer, tempPollInterval; // Define at top for access in shutdown handlers
let tempPollInProgress = false;
let shutdownInProgress = false;

function getEnvInt(name, defaultValue) {
  const value = parseInt(process.env[name], 10);
  return Number.isInteger(value) && value > 0 ? value : defaultValue;
}

function getEnvBool(name, defaultValue = false) {
  const value = process.env[name];

  if (value === undefined) {
    return defaultValue;
  }

  return ["1", "true", "yes"].includes(value.toLowerCase());
}

function getPoolOptions(prefix) {
  return {
    pool: {
      connectionLimit: getEnvInt(`${prefix}_DB_CONNECTION_LIMIT`, 5),
      maxIdle: getEnvInt(`${prefix}_DB_MAX_IDLE`, 2),
      idleTimeout: getEnvInt(`${prefix}_DB_IDLE_TIMEOUT_MS`, 30_000),
      queueLimit: getEnvInt(`${prefix}_DB_QUEUE_LIMIT`, 50),
      connectTimeout: getEnvInt(`${prefix}_DB_CONNECT_TIMEOUT_MS`, 10_000),
    },
    slowQueryThresholdMs: getEnvInt("DB_SLOW_QUERY_THRESHOLD_MS", 2_000),
    poolDebug: getEnvBool("DB_POOL_DEBUG"),
  };
}

async function shutdown(signal, exitCode = 0) {
  if (shutdownInProgress) {
    return;
  }

  shutdownInProgress = true;
  console.log(`🛑 Received ${signal}. Closing servers and database pools...`);

  if (tempPollInterval) {
    clearInterval(tempPollInterval);
    tempPollInterval = null;
  }

  if (udpServer) {
    udpServer.close();
  }

  if (httpServer) {
    httpServer.close();
  }

  await Promise.allSettled([
    dbManager?.close(),
    serverDbManager?.close(),
  ]);

  process.exit(exitCode);
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

async function startApplication() {
  try {
    const dbConfig = {
      host: process.env.LOCAL_HOST || "localhost",
      user: process.env.LOCAL_USER || "root",
      password: process.env.LOCAL_PASSWORD || "123",
      database: process.env.LOCAL_DB || "rfid_db",
      port: parseInt(process.env.LOCAL_PORT, 10) || 3306,
    };

    const serverDbConfig = {
      host: process.env.SERVER_HOST,
      user: process.env.SERVER_USER,
      password: process.env.SERVER_PASSWORD,
      database: process.env.SERVER_DB,
      port: parseInt(process.env.SERVER_PORT, 10) || 3306,
    };

    if (!serverDbConfig.host || !serverDbConfig.user || !serverDbConfig.password || !serverDbConfig.database) {
      throw new Error("Missing one or more required SERVER_ environment variables.");
    }

    // Initialize DB managers with pooling
    dbManager = new DatabaseManager(dbConfig, { name: "local", ...getPoolOptions("LOCAL") });
    serverDbManager = new DatabaseManager(serverDbConfig, { name: "server", ...getPoolOptions("SERVER") });

    await dbManager.connect();

    try {
      await serverDbManager.connect();
      console.log("✅ Both databases connected successfully.");
    } catch (err) {
      if (getEnvBool("SERVER_DB_REQUIRED")) {
        throw err;
      }

      console.warn(
        "⚠️ Server database is not reachable at startup; continuing with local DB and retrying server sync later:",
        err.message
      );
    }

    // Start UDP server
    const udpPort = parseInt(process.env.UDP_PORT, 10) || 5001;
    udpServer = new UDPServer(dbManager, serverDbManager, {
      port: udpPort,
      host: "0.0.0.0",
    });
    udpServer.start();
    console.log(`📡 UDP server started on port ${udpPort}`);

    // Start HTTP server
    const httpPort = parseInt(process.env.HTTP_PORT, 10) || 5002;
    const url = require("url");

    const sslOptions = {
      key: fs.readFileSync("/home/ubuntu/ssl/key.pem"),
      cert: fs.readFileSync("/home/ubuntu/ssl/cert.pem")
    };

    // const httpsPort = parseInt(process.env.HTTPS_PORT, 10) || 8443;

    // const httpServer = require("http").createServer(async (req, res) => {
    //   const parsedUrl = url.parse(req.url, true);

    // });
    httpServer = require("http").createServer(async (req, res) => {
      const parsedUrl = url.parse(req.url, true);

      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");

      if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
      }

      if (req.method === "GET" && parsedUrl.pathname === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "ok" }));
        return;
      }

      if (req.method === "GET" && parsedUrl.pathname === "/fetchtemp") {
        try {
          const query = `
        SELECT 
  \`date\`,
  \`intemp\` AS \`intemp_1\`,
  \`middletemp\` AS \`middletemp_1\`,
  \`outtemp\` AS \`outtemp_1\`,
  \`pressure\` AS \`pressure_1\`,
  0 AS \`intemp_2\`,
  0 AS \`middletemp_2\`,
  0 AS \`outtemp_2\`,
  0 AS \`pressure_2\`
FROM rfid_db.temperature_line
ORDER BY id DESC
LIMIT 1;
      `;

          const rows = await dbManager.query(query);

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(rows[0] || {}));
          return;

        } catch (err) {
          console.error("❌ Failed to fetch temperature data:", err.message);

          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({
            error: "Failed to fetch temperature data",
            details: err.message
          }));
          return;
        }
      }

      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not Found" }));
    });

    // const httpsServer = https.createServer(sslOptions, async (req, res) => {
    //   const parsedUrl = url.parse(req.url, true);

    //   res.setHeader("Access-Control-Allow-Origin", "*");
    //   res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    //   res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    //   if (req.method === "GET" && parsedUrl.pathname === "/health") {
    //     res.writeHead(200, { "Content-Type": "application/json" });
    //     res.end(JSON.stringify({ status: "ok" }));
    //   } else if (req.method === "GET" && parsedUrl.pathname === "/latest-temperature") {
    //     try {
    //       const [rows] = await dbManager.pool.query(`SELECT * FROM temperature_line ORDER BY date DESC LIMIT 1`);


    //       res.writeHead(200, { "Content-Type": "application/json" });
    //       res.end(JSON.stringify(rows[0] || {}));
    //     } catch (err) {
    //       res.writeHead(500, { "Content-Type": "application/json" });
    //       res.end(JSON.stringify({ error: "Failed to fetch latest data", details: err.message }));
    //     }
    //   } else {
    //     res.writeHead(404, { "Content-Type": "application/json" });
    //     res.end(JSON.stringify({ error: "Not Found" }));
    //   }
    // });



    // httpsServer.listen(httpsPort, () => {
    //   console.log(`🔐 HTTPS server listening on port ${httpsPort}`);
    // });

    httpServer.listen(httpPort, () => {
      console.log(`🌐 HTTP server listening on port ${httpPort}`);
    });

    // Start polling every 1 minute
    tempPollInterval = setInterval(pollExternalControllers, 60 * 1000);
    pollExternalControllers();

  } catch (error) {
    console.error("❌ Initialization error:", error);
    await shutdown("initialization error", 1);
  }
}

process.on("unhandledRejection", async (err) => {
  console.error("🔴 Unhandled rejection:", err);
  await shutdown("unhandledRejection", 1);
});

// 🛰️ Periodic fetch from external controller IP
const pollExternalControllers = async () => {
  if (tempPollInProgress) {
    console.warn("⏳ Previous temperature poll is still running; skipping this interval");
    return;
  }

  tempPollInProgress = true;

  try {
    const response = await axios.get("http://192.168.2.218", { timeout: 10_000 });
    const data = response.data;

    await insertTemperatureLine(data);
    console.log(`[${new Date().toISOString()}] ✅ Controller data inserted`);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] ❌ Failed to fetch or insert data:`, error.message);
  } finally {
    tempPollInProgress = false;
  }
};

const insertTemperatureLine = async (data) => {
  const timestamp = data.timestamp;
  const controllers = data.controllers;

  const inVal = controllers["1"]?.pv ?? 0;
  const midVal = controllers["2"]?.pv ?? 0;
  const outVal = controllers["3"]?.pv ?? 0;
  const pressureVal = controllers["4"]?.pv ?? 0;

  const query = `
    INSERT INTO temperature_line (\`date\`, \`intemp\`, \`middletemp\`, \`outtemp\`, \`pressure\`)
    VALUES (?, ?, ?, ?, ?)
  `;

  await dbManager.query(query, [timestamp, inVal, midVal, outVal, pressureVal]);
};

startApplication();
