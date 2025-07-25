// udpServer.js
const dgram = require("dgram");
const DatabaseManager = require("./databaseManager");
const moment = require('moment');

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

    this.server.on("message", (msg, rinfo) => {
      // console.log(msg, rinfo);
      this._handleMessage(msg, rinfo)
    });

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
        // console.log("Received outtime message");
        // console.log(`Outtime message: ${cleanMessage}`);
        await this._handleOuttimeUpdate(cleanMessage);
      } else if (ip === "192.168.2.216") {
        // console.log("Received intime message");
        // console.log(`Intime message: ${cleanMessage}`);
        await this._handleIntimeInsert(cleanMessage);

      } else {
        console.log(`Unhandled IP address: ${ip}`);
      }
    } catch (error) {
      console.error("Error handling UDP message:", error);
    }
  }

  async _handleIntimeInsert(mouldId) {
    try {
      const checkQuery = `SELECT * FROM rfid_db.rfid WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL`;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        console.log("Row exists with intime but outtime null, ignoring insert");
        return;
      }

      const insertQuery = `INSERT INTO rfid_db.rfid (mouldId, intime, outtime, creadt, sync_status) VALUES (?, NOW(), NULL, NOW(), 'pending')`;
      const result = await this.dbManager.query(insertQuery, [mouldId]);
      const insertedId = result.insertId;

      try {
        const dateTimeNow = moment(new Date(), 'YYYY-MM-DD HH:mm:ss');
        const serverInsertQuery = `INSERT INTO hexsys.rfid (id, mouldId, intime, outtime, creadt) VALUES (?, ?, ${dateTimeNow}, NULL, ${dateTimeNow})`;
        await this.serverDbManager.query(serverInsertQuery, [insertedId, mouldId]);
        await this.dbManager.query(`UPDATE rfid_db.rfid SET sync_status = 'synced' WHERE id = ?`, [insertedId]);
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
      const checkQuery = `SELECT * FROM rfid_db.rfid WHERE mouldId = ? AND intime IS NOT NULL AND outtime IS NULL ORDER BY id DESC LIMIT 1`;
      const results = await this.dbManager.query(checkQuery, [mouldId]);

      if (results.length > 0) {
        const { id, intime, outtime } = results[0];
        const searchTemp = await this.dbManager.query(`
          SELECT 
            ROUND(MIN(intemp), 2) in_min,
            ROUND(MAX(intemp), 2) in_max,
            ROUND(AVG(intemp), 2) in_avg,
            ROUND(MIN(middletemp), 2) mid_min,
            ROUND(MAX(middletemp), 2) mid_max,
            ROUND(AVG(middletemp), 2) mid_avg,
            ROUND(MIN(outtemp), 2) out_min,
            ROUND(MAX(outtemp), 2) out_max,
            ROUND(AVG(outtemp), 2) out_avg,
            ROUND(AVG(pressure), 2) pressure,
            ROUND(MIN(pressure), 2) pressure_min,
            ROUND(MAX(pressure), 2) pressure_max
            FROM
                rfid_db.temperature_line
            WHERE
                (date BETWEEN '${intime}' AND NOW());`);
        const tempLogs = await this.dbManager.query(`
          WITH time_buckets AS (
              SELECT  
                  DATE_ADD('${intime}', INTERVAL (seq * 3) MINUTE) AS bucket_time
              FROM (
                  SELECT @row := @row + 1 AS seq
                  FROM information_schema.columns, (SELECT @row := -1) AS init
                  LIMIT 1000  -- Adjust based on max time range in minutes / 3
              ) AS seq_gen
          ),
          closest_logs AS (
              SELECT 
                  t.id,
                  t.date,
                  t.intemp,
                  t.middletemp,
                  t.outtemp,
                  t.pressure,
                  tb.bucket_time,
                  ABS(TIMESTAMPDIFF(SECOND, t.date, tb.bucket_time)) AS time_diff,
                  ROW_NUMBER() OVER (PARTITION BY tb.bucket_time ORDER BY ABS(TIMESTAMPDIFF(SECOND, t.date, tb.bucket_time))) AS rn
              FROM 
                  time_buckets tb
              JOIN 
                  rfid_db.temperature_line t
                  ON t.date BETWEEN '${intime}' AND NOW()
                  AND t.intemp > 5
                  AND t.middletemp > 5
                  AND t.outtemp > 5
                  AND t.pressure > 0
                  AND t.date BETWEEN DATE_SUB(tb.bucket_time, INTERVAL 90 SECOND)
                                  AND DATE_ADD(tb.bucket_time, INTERVAL 90 SECOND)
          )
          SELECT 
              id, date, intemp, middletemp, outtemp, pressure, bucket_time
          FROM 
              closest_logs
          WHERE 
              rn = 1
          ORDER BY 
              date;
          `);
          console.log("Temperature Logs:", tempLogs);
          let tempLogsJson = '[]';
          if(tempLogs?.length > 0) {
            // json stringify temperature logs
            tempLogsJson = JSON.stringify(tempLogs);
            console.log("Temperature Logs:", tempLogsJson);
          }

        const updateQuery = `UPDATE rfid_db.rfid SET outtime = NOW(), sync_status = 'pending',
          in_min = ${searchTemp[0].in_min},
          in_max = ${searchTemp[0].in_max},
          in_avg = ${searchTemp[0].in_avg},
          mid_min = ${searchTemp[0].mid_min},
          mid_max = ${searchTemp[0].mid_max},
          mid_avg = ${searchTemp[0].mid_avg},
          out_min = ${searchTemp[0].out_min},
          out_max = ${searchTemp[0].out_max},
          out_avg = ${searchTemp[0].out_avg},
          pressure = ${searchTemp[0].pressure},
          pressure_min = ${searchTemp[0].pressure_min},
          pressure_max = ${searchTemp[0].pressure_max},
          templogs = '${tempLogsJson}'
        WHERE id = ?`;
        await this.dbManager.query(updateQuery, [id]);

        try {
          const dateTimeNow = moment(new Date(), 'YYYY-MM-DD HH:mm:ss');
          const serverUpdateQuery = `UPDATE hexsys.rfid SET outtime = ${dateTimeNow},
            in_min = ${searchTemp[0].in_min},
            in_max = ${searchTemp[0].in_max},
            in_avg = ${searchTemp[0].in_avg},
            mid_min = ${searchTemp[0].mid_min},
            mid_max = ${searchTemp[0].mid_max},
            mid_avg = ${searchTemp[0].mid_avg},
            out_min = ${searchTemp[0].out_min},
            out_max = ${searchTemp[0].out_max},
            out_avg = ${searchTemp[0].out_avg},
            pressure = ${searchTemp[0].pressure},
            pressure_min = ${searchTemp[0].pressure_min},
            pressure_max = ${searchTemp[0].pressure_max},
            templogs = '${tempLogsJson}'
          WHERE id = ?`;
          await this.serverDbManager.query(serverUpdateQuery, [id]);
          await this.dbManager.query(`UPDATE rfid_db.rfid SET sync_status = 'synced' WHERE id = ?`, [id]);
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
      const pendingRecords = await this.dbManager.query(`SELECT * FROM rfid_db.rfid WHERE sync_status = 'pending'`);

      for (const row of pendingRecords) {
        const { id, mouldId, intime, outtime, creadt, in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs } = row;
        const query = `
          INSERT INTO hexsys.rfid (id, mouldId, intime, outtime,
            in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs, creadt)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            mouldId = VALUES(mouldId),
            intime = VALUES(intime),
            outtime = VALUES(outtime),
            in_min = VALUES(in_min),
            in_max = VALUES(in_max),
            in_avg = VALUES(in_avg),
            mid_min = VALUES(mid_min),
            mid_max = VALUES(mid_max),
            mid_avg = VALUES(mid_avg),
            out_min = VALUES(out_min),
            out_max = VALUES(out_max),
            out_avg = VALUES(out_avg),
            pressure = VALUES(pressure),
            pressure_min = VALUES(pressure_min),
            pressure_max = VALUES(pressure_max),
            templogs = VALUES(templogs),
            creadt = VALUES(creadt)
        `;

        try {
          await this.serverDbManager.query(query, [id, mouldId, intime, outtime, in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs, creadt]);
          await this.dbManager.query(`UPDATE rfid_db.rfid SET sync_status = 'synced' WHERE id = ?`, [id]);
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