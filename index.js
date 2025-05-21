require("dotenv").config(); // Load env vars at the top

const DatabaseManager = require("./databaseManager");
const UDPServer = require("./udpServer");
const http = require("http");

async function main() {
  // MySQL database configuration
  const dbConfig = {
    host: process.env.LOCAL_HOST,
    user: process.env.LOCAL_USER,
    password: process.env.LOCAL_PASSWORD,
    database: process.env.LOCAL_DB,
    port: parseInt(process.env.LOCAL_PORT),
  };

  const serverDbConfig = {
    host: process.env.SERVER_HOST,
    user: process.env.SERVER_USER,
    password: process.env.SERVER_PASSWORD,
    database: process.env.SERVER_DB,
    port: parseInt(process.env.SERVER_PORT),
  };

  const dbManager = new DatabaseManager(dbConfig);
  const serverDbManager = new DatabaseManager(serverDbConfig);

  try {
    await dbManager.connect();
    await serverDbManager.connect();

    const udpServer = new UDPServer(dbManager, serverDbManager, { port: 5001, host: "0.0.0.0" });
    udpServer.start();

    const httpServer = http.createServer((req, res) => {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("HTTP server is running on port 5000");
    });

    httpServer.listen(5002, () => {
      console.log("HTTP server listening on port 5000");
    });
  } catch (error) {
    console.error("Initialization error:", error);
  }
}

main();
