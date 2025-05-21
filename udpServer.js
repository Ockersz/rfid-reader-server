// udpServer.js
const dgram = require("dgram");
const DatabaseManager = require("./databaseManager");

/**
 * UDPServer handles incoming UDP messages and updates the database accordingly.
 */
class UDPServer {
  constructor(dbManager, serverDbManager, options = {}) {
    this.dbManager = dbManager;
    this.serverDbManager = serverDbManager;
    this.port = options.port || 5001;
    this.host = options.host || "0.0.0.0";
    this.server = dgram.createSocket("udp4");
    this._setupListeners();
    this._startSyncJob();
  }

  _setupListeners() {
    this.server.on("error", (err) => {
      console.error(`Server error: ${err.stack}`);
      this.server.close();
    });

    this.server.on("message", (msg, rinfo) => this._handleMessage(msg, rinfo));

    this.server.on("listening", () => {
      const address = this.server.address();
      console.log(`UDP server listening on ${address.address}:${address.port}`);
    });
  }

  async _handleMessage(msg, rinfo) {
    const ip = rinfo.address;
    const rawMessage = msg.toString().trim();
    const cleanMessage = rawMessage.replace(/[@$]/g, "");
    console.log(`Received message from ${ip}:${rinfo.port}: ${cleanMessage}`);

    try {
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

  async _handleIntimeInsert(mouldId) {
    try {
      const checkQuery = `SELECT * FROM hexsys.rfid WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL`;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        console.log("Row exists with intime but outtime null, ignoring insert");
        return;
      }

      const insertQuery = `INSERT INTO hexsys.rfid (mouldId, intime, outtime, creadt, sync_status) VALUES (?, NOW(), NULL, NOW(), 'pending')`;
      const result = await this.dbManager.query(insertQuery, [mouldId]);
      const insertedId = result.insertId;

      try {
        const serverInsertQuery = `INSERT INTO hexsys.rfid (id, mouldId, intime, outtime, creadt) VALUES (?, ?, NOW(), NULL, NOW())`;
        await this.serverDbManager.query(serverInsertQuery, [insertedId, mouldId]);
        await this.dbManager.query(`UPDATE hexsys.rfid SET sync_status = 'synced' WHERE id = ?`, [insertedId]);
      } catch (err) {
        console.warn("Server DB sync failed for insert. Will retry later.");
      }

      console.log("Inserted new row with intime:", mouldId);
    } catch (error) {
      console.error("Error during intime insertion:", error);
    }
  }

  async _handleOuttimeUpdate(mouldId) {
    try {
      const checkQuery = `SELECT * FROM hexsys.rfid WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL ORDER BY id DESC LIMIT 1`;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        const { id } = results[0];
        const updateQuery = `UPDATE hexsys.rfid SET outtime = NOW(), sync_status = 'pending' WHERE id = ?`;
        await this.dbManager.query(updateQuery, [id]);

        try {
          const serverUpdateQuery = `UPDATE hexsys.rfid SET outtime = NOW() WHERE id = ?`;
          await this.serverDbManager.query(serverUpdateQuery, [id]);
          await this.dbManager.query(`UPDATE hexsys.rfid SET sync_status = 'synced' WHERE id = ?`, [id]);
        } catch (err) {
          console.warn("Server DB sync failed for update. Will retry later.");
        }

        console.log("Updated row with outtime:", mouldId);
      } else {
        console.log("No active record found for mouldId:", mouldId);
      }
    } catch (error) {
      console.error("Error during outtime update:", error);
    }
  }

  async _syncPendingRecords() {
    try {
      const pendingRecords = await this.dbManager.query(`SELECT * FROM hexsys.rfid WHERE sync_status = 'pending'`);

      for (const row of pendingRecords) {
        const { id, mouldId, intime, outtime, creadt } = row;
        const query = `
          INSERT INTO hexsys.rfid (id, mouldId, intime, outtime, creadt)
          VALUES (?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            mouldId = VALUES(mouldId),
            intime = VALUES(intime),
            outtime = VALUES(outtime),
            creadt = VALUES(creadt)
        `;

        try {
          await this.serverDbManager.query(query, [id, mouldId, intime, outtime, creadt]);
          await this.dbManager.query(`UPDATE hexsys.rfid SET sync_status = 'synced' WHERE id = ?`, [id]);
          console.log(`✅ Synced record ID: ${id}`);
        } catch (err) {
          console.warn(`❌ Sync failed for record ID ${id}:`, err.message);
        }
      }
    } catch (err) {
      console.error("Sync job failed:", err);
    }
  }

  _startSyncJob() {
    setInterval(() => this._syncPendingRecords(), 60 * 1000);
  }

  start() {
    this.server.bind(this.port, this.host);
  }
}

module.exports = UDPServer;