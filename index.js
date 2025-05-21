require("dotenv").config();
const http = require("http");
const DatabaseManager = require("./databaseManager");
const UDPServer = require("./udpServer");

async function startApplication() {
  try {
    const dbConfig = {
      host: process.env.LOCAL_HOST || "localhost",
      user: process.env.LOCAL_USER || "root",
      password: process.env.LOCAL_PASSWORD || "123",
      database: process.env.LOCAL_DB || "hexsys",
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
    const dbManager = new DatabaseManager(dbConfig);
    const serverDbManager = new DatabaseManager(serverDbConfig);

    // Optional: ping databases to test connectivity
    await dbManager.connect();
    await serverDbManager.connect();
    console.log("✅ Both databases connected successfully.");

    // Start UDP server
    const udpPort = parseInt(process.env.UDP_PORT, 10) || 5001;
    const udpServer = new UDPServer(dbManager, serverDbManager, {
      port: udpPort,
      host: "0.0.0.0",
    });
    udpServer.start();
    console.log(`📡 UDP server started on port ${udpPort}`);

    // Start HTTP server
    const httpPort = parseInt(process.env.HTTP_PORT, 10) || 5002;
    const httpServer = http.createServer((req, res) => {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("HTTP server is running");
    });

    httpServer.listen(httpPort, () => {
      console.log(`🌐 HTTP server listening on port ${httpPort}`);
    });

    // Graceful shutdown (optional)
    process.on("SIGINT", async () => {
      console.log("Shutting down...");
      await dbManager.close();
      await serverDbManager.close();
      process.exit(0);
    });

  } catch (error) {
    console.error("❌ Initialization error:", error);
    process.exit(1);
  }
}

process.on("unhandledRejection", (err) => {
  console.error("🔴 Unhandled rejection:", err);
  process.exit(1);
});

startApplication();
