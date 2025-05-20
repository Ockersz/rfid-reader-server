// index.js
const DatabaseManager = require("./databaseManager");
const UDPServer = require("./udpServer");
const http = require("http");

async function main() {
  // MySQL database configuration
  const dbConfig = {
    host: "localhost",
    user: "root",
    password: "123",
    database: "hexsys",
    port: 3306,
  };

  const serverDbConfig = {
    host: "44.199.44.200",
    user: "admin",
    password: "HexSys@Earth#foam2023",
    database: "hexsys",
    port: 3306,
  };

  // Instantiate the DatabaseManager
  const dbManager = new DatabaseManager(dbConfig);
  const serverDbManager = new DatabaseManager(serverDbConfig);

  try {
    // Connect to the database
    await dbManager.connect();
    await serverDbManager.connect();

    // Create and start the UDP server on port 5001
    const udpServer = new UDPServer(dbManager, serverDbManager, { port: 5001, host: "0.0.0.0" });
    udpServer.start();

    // Create and start an HTTP server on port 5000
    const httpServer = http.createServer((req, res) => {
      // Send a plain text response
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

// Start the application
main();
