// udpServer.js
const dgram = require("dgram");
const DatabaseManager = require("./databaseManager");

/**
 * UDPServer handles incoming UDP messages and updates the database accordingly.
 */
class UDPServer {
  /**
   * @param {DatabaseManager} dbManager - Instance of DatabaseManager.
   * @param {DatabaseManager} serverDbManager - Instance of server database
   * @param {Object} options - Server configuration options.
   */
  constructor(dbManager, serverDbManager, options = {}) {
    this.dbManager = dbManager;
    this.serverDbManager = serverDbManager;
    this.port = options.port || 5001;
    this.host = options.host || "0.0.0.0"; // Listen on all network interfaces
    this.server = dgram.createSocket("udp4");
    this._setupListeners();
  }

  /**
   * Sets up event listeners for error handling, incoming messages, and server start.
   * @private
   */
  _setupListeners() {
    // Log server errors and close the server if one occurs
    this.server.on("error", (err) => {
      console.error(`Server error: ${err.stack}`);
      this.server.close();
    });

    // Process each incoming UDP message
    this.server.on("message", (msg, rinfo) => this._handleMessage(msg, rinfo));

    // Log when the server starts listening
    this.server.on("listening", () => {
      const address = this.server.address();
      console.log(`UDP server listening on ${address.address}:${address.port}`);
    });
  }

  /**
   * Processes the received message and routes it based on the sender's IP.
   * @param {Buffer} msg - The received UDP message.
   * @param {Object} rinfo - Remote client information.
   * @private
   */
  async _handleMessage(msg, rinfo) {
    const ip = rinfo.address;
    // Clean the message by removing unwanted characters
    const rawMessage = msg.toString().trim();
    const cleanMessage = rawMessage.replace(/[@$]/g, "");
    console.log(`Received message from ${ip}:${rinfo.port}: ${cleanMessage}`);

    try {
      // Process message based on sender's IP address
      if (ip === "192.168.2.215") {
        await this._handleIntimeInsert(cleanMessage);
      } else if (ip === "192.168.2.216") {
        await this._handleOuttimeUpdate(cleanMessage);
      } else {
        console.log(`Unhandled IP address: ${ip}`);
      }
    } catch (error) {
      console.error("Error handling UDP message:", error);
    }
  }

  /**
   * Inserts a new row with the current time as 'intime' if no active record exists.
   * @param {string} mouldId - The identifier from the UDP message.
   * @private
   */
  async _handleIntimeInsert(mouldId) {
    try {
      const checkQuery = `
        SELECT * FROM hexsys.rfid
        WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL
      `;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        console.log("Row exists with intime but outtime null, ignoring insert");
      } else {
        const insertQuery = `
          INSERT INTO hexsys.rfid (mouldId, intime, outtime, creadt)
          VALUES (?, NOW(), NULL, NOW())
        `;
        await this.dbManager.query(insertQuery, [mouldId]);
        await this.serverDbManager.query(insertQuery, [mouldId]);
        console.log("Inserted new row with intime:", mouldId);
      }
    } catch (error) {
      console.error("Error during intime insertion:", error);
    }
  }

  /**
   * Updates an existing row by setting the current time as 'outtime'.
   * @param {string} mouldId - The identifier from the UDP message.
   * @private
   */
  async _handleOuttimeUpdate(mouldId) {
    try {
      const checkQuery = `
        SELECT * FROM hexsys.rfid
        WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL
      `;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        const updateQuery = `
          UPDATE hexsys.rfid
          SET outtime = NOW()
          WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL
        `;
        await this.dbManager.query(updateQuery, [mouldId]);
        await this.serverDbManager.query(updateQuery, [mouldId]);
        console.log("Updated row with outtime:", mouldId);
      } else {
        console.log("No active record found for mouldId:", mouldId);
      }
    } catch (error) {
      console.error("Error during outtime update:", error);
    }
  }

  /**
   * Starts the UDP server.
   */
  start() {
    this.server.bind(this.port, this.host);
  }
}

module.exports = UDPServer;
