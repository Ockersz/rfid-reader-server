// udpServer.js
const dgram = require("dgram");
const DatabaseManager = require("./databaseManager");
const moment = require('moment');

const RFID_DEVICES = {
  "192.168.2.215": { action: "out", line: "1" },
  "192.168.2.216": { action: "in", line: "1" },
  "192.168.2.222": { action: "in", line: "2" },
  "192.168.2.223": { action: "out", line: "2" },
};

const TEMPERATURE_COLUMNS_BY_LINE = {
  "1": {
    in: "intemp",
    middle: "middletemp",
    out: "outtemp",
    pressure: "pressure",
  },
  "2": {
    in: "intemp2",
    middle: "middletemp2",
    out: "outtemp2",
    pressure: "pressure2",
  },
};

function getTemperatureColumns(line) {
  return TEMPERATURE_COLUMNS_BY_LINE[line] || TEMPERATURE_COLUMNS_BY_LINE["1"];
}

function normalizeTempStats(row = {}) {
  return {
    in_min: row.in_min ?? null,
    in_max: row.in_max ?? null,
    in_avg: row.in_avg ?? null,
    mid_min: row.mid_min ?? null,
    mid_max: row.mid_max ?? null,
    mid_avg: row.mid_avg ?? null,
    out_min: row.out_min ?? null,
    out_max: row.out_max ?? null,
    out_avg: row.out_avg ?? null,
    pressure: row.pressure ?? null,
    pressure_min: row.pressure_min ?? null,
    pressure_max: row.pressure_max ?? null,
  };
}

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
    this.syncInProgress = false;
    this.syncInterval = null;
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
    // console.log(`Received message from ${ip}:${rinfo.port}: ${cleanMessage}`);

    try {
      const device = RFID_DEVICES[ip];

      if (!device) {
        console.log(`Unhandled IP address: ${ip}`);
        return;
      }

      if (device.action === "out") {
        await this._handleOuttimeUpdate(cleanMessage, device.line);
      } else if (device.action === "in") {
        await this._handleIntimeInsert(cleanMessage, device.line);
      }
    } catch (error) {
      console.error("Error handling UDP message:", error);
    }
  }

  async _handleIntimeInsert(mouldId, line = "1") {
    try {
      const checkQuery = `
        SELECT *
        FROM rfid_db.rfid
        WHERE mouldId = ?
          AND COALESCE(\`line\`, '1') = ?
          AND intime IS NOT NULL
          AND outtime IS NULL
      `;
      const results = await this.dbManager.query(checkQuery, [mouldId, line]);

      if (results.length > 0) {
        // console.log("Row exists with intime but outtime null, ignoring insert");
        return;
      }

      const insertQuery = `
        INSERT INTO rfid_db.rfid (mouldId, intime, outtime, creadt, sync_status, \`line\`)
        VALUES (?, NOW(), NULL, NOW(), 'pending', ?)
      `;
      const result = await this.dbManager.query(insertQuery, [mouldId, line]);
      const insertedId = result.insertId;

      try {
        const serverInsertQuery = `
          INSERT INTO hexsys.rfid (id, mouldId, intime, outtime, creadt, \`line\`)
          VALUES (?, ?, NOW(), NULL, NOW(), ?)
        `;
        await this.serverDbManager.query(serverInsertQuery, [insertedId, mouldId, line]);
        await this.dbManager.query(`UPDATE rfid_db.rfid SET sync_status = 'synced' WHERE id = ?`, [insertedId]);
      } catch (err) {
        console.warn("Server DB sync failed for insert. Will retry later.");
      }

      console.log(`Inserted new row with intime for line ${line}:`, mouldId);
    } catch (error) {
      console.error("Error during intime insertion:", error);
    }
  }

  async _handleOuttimeUpdate(mouldId, line = "1") {
    try {
      const checkQuery = `SELECT a.*,
        (SELECT 
            COUNT(*)
        FROM
            rfid r
        WHERE
            r.mouldId = a.mouldId
                AND COALESCE(r.\`line\`, '1') = COALESCE(a.\`line\`, '1')
                AND r.intime BETWEEN CASE
                WHEN
                    TIME(a.intime) >= '07:00:00'
                        AND TIME(a.intime) < '19:00:00'
                THEN
                    DATE_SUB(DATE(a.intime), INTERVAL 0 DAY) + INTERVAL '07:00:00' HOUR_SECOND
                ELSE CASE
                    WHEN TIME(a.intime) >= '19:00:00' THEN DATE(a.intime) + INTERVAL '19:00:00' HOUR_SECOND
                    ELSE DATE_SUB(DATE(a.intime), INTERVAL 1 DAY) + INTERVAL '19:00:00' HOUR_SECOND
                END
            END AND a.intime) AS cycle_count
        FROM rfid_db.rfid a
        WHERE mouldId = ?
          AND COALESCE(\`line\`, '1') = ?
          AND intime IS NOT NULL
          AND outtime IS NULL
        ORDER BY id DESC
        LIMIT 1`;
      const results = await this.dbManager.query(checkQuery, [mouldId, line]);

      if (results.length > 0) {
        const { id, intime, cycle_count } = results[0];
        const intimeFormatted = moment(intime).format('YYYY-MM-DD HH:mm:ss');
        const temperatureColumns = getTemperatureColumns(line);

        const queryForTem = `
  SELECT 
    ROUND(MIN(${temperatureColumns.in}), 2) in_min,
    ROUND(MAX(${temperatureColumns.in}), 2) in_max,
    ROUND(AVG(${temperatureColumns.in}), 2) in_avg,
    ROUND(MIN(${temperatureColumns.middle}), 2) mid_min,
    ROUND(MAX(${temperatureColumns.middle}), 2) mid_max,
    ROUND(AVG(${temperatureColumns.middle}), 2) mid_avg,
    ROUND(MIN(${temperatureColumns.out}), 2) out_min,
    ROUND(MAX(${temperatureColumns.out}), 2) out_max,
    ROUND(AVG(${temperatureColumns.out}), 2) out_avg,
    ROUND(AVG(${temperatureColumns.pressure}), 2) pressure,
    ROUND(MIN(${temperatureColumns.pressure}), 2) pressure_min,
    ROUND(MAX(${temperatureColumns.pressure}), 2) pressure_max
  FROM rfid_db.temperature_line
  WHERE 
    date BETWEEN ? AND NOW()
    AND (${temperatureColumns.in} > 5 AND ${temperatureColumns.middle} > 5 AND ${temperatureColumns.out} > 5 AND ${temperatureColumns.pressure} > 1 AND ${temperatureColumns.pressure} < 10);`;

        console.log(queryForTem);
        const searchTemp = await this.dbManager.query(queryForTem, [intimeFormatted]);
        const tempStats = normalizeTempStats(searchTemp[0]);
        console.log("Temperature search results:", searchTemp);
        const tempLogs = await this._getTemperatureLogs(intimeFormatted, line);
        const tempLogsJson = tempLogs?.length > 0 ? JSON.stringify(tempLogs) : '[]';

        const updateQuery = `UPDATE rfid_db.rfid SET outtime = NOW(), sync_status = 'pending',
          in_min = ?,
          in_max = ?,
          in_avg = ?,
          mid_min = ?,
          mid_max = ?,
          mid_avg = ?,
          out_min = ?,
          out_max = ?,
          out_avg = ?,
          pressure = ?,
          pressure_min = ?,
          pressure_max = ?,
          templogs = ?,
          cycle = ?,
          \`line\` = ?
        WHERE id = ?`;
        await this.dbManager.query(updateQuery, [
          tempStats.in_min,
          tempStats.in_max,
          tempStats.in_avg,
          tempStats.mid_min,
          tempStats.mid_max,
          tempStats.mid_avg,
          tempStats.out_min,
          tempStats.out_max,
          tempStats.out_avg,
          tempStats.pressure,
          tempStats.pressure_min,
          tempStats.pressure_max,
          tempLogsJson,
          cycle_count,
          line,
          id,
        ]);

        try {
          const serverUpdateQuery = `UPDATE hexsys.rfid SET outtime = NOW(),
            in_min = ?,
            in_max = ?,
            in_avg = ?,
            mid_min = ?,
            mid_max = ?,
            mid_avg = ?,
            out_min = ?,
            out_max = ?,
            out_avg = ?,
            pressure = ?,
            pressure_min = ?,
            pressure_max = ?,
            templogs = ?,
            cycle = ?,
            \`line\` = ?
          WHERE id = ?`;
          await this.serverDbManager.query(serverUpdateQuery, [
            tempStats.in_min,
            tempStats.in_max,
            tempStats.in_avg,
            tempStats.mid_min,
            tempStats.mid_max,
            tempStats.mid_avg,
            tempStats.out_min,
            tempStats.out_max,
            tempStats.out_avg,
            tempStats.pressure,
            tempStats.pressure_min,
            tempStats.pressure_max,
            tempLogsJson,
            cycle_count,
            line,
            id,
          ]);
          await this.dbManager.query(`UPDATE rfid_db.rfid SET sync_status = 'synced' WHERE id = ?`, [id]);
        } catch (err) {
          console.warn("Server DB sync failed for update. Will retry later.");
        }

        console.log(`Updated row with outtime for line ${line}:`, mouldId);
      } else {
        // console.log("No active record found for mouldId:", mouldId);
      }
    } catch (error) {
      console.error("Error during outtime update:", error);
    }
  }

  async _getTemperatureLogs(intimeFormatted, line) {
    const temperatureColumns = getTemperatureColumns(line);
    const query = `
      SELECT
        date,
        ${temperatureColumns.in} AS intemp,
        ${temperatureColumns.middle} AS middletemp,
        ${temperatureColumns.out} AS outtemp,
        ${temperatureColumns.pressure} AS pressure
      FROM rfid_db.temperature_line
      WHERE date BETWEEN ? AND NOW()
      ORDER BY date ASC
    `;

    return this.dbManager.query(query, [intimeFormatted]);
  }

  async _syncPendingRecords() {
    try {
      const pendingRecords = await this.dbManager.query(`SELECT * FROM rfid_db.rfid WHERE sync_status = 'pending'`);

      for (const row of pendingRecords) {
        const { id, mouldId, intime, outtime, creadt, in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs, cycle, line } = row;
        const query = `
          INSERT INTO hexsys.rfid (id, mouldId, intime, outtime,
            in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs, creadt, cycle, \`line\`)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            creadt = VALUES(creadt),
            cycle = VALUES(cycle),
            \`line\` = VALUES(\`line\`)
        `;

        try {
          await this.serverDbManager.query(query, [id, mouldId, intime, outtime, in_min, in_max, in_avg, mid_min, mid_max, mid_avg, out_min, out_max, out_avg, pressure, pressure_min, pressure_max, templogs, creadt, cycle, line || "1"]);
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
    this.syncInterval = setInterval(async () => {
      if (this.syncInProgress) {
        console.warn("⏳ Previous sync job is still running; skipping this interval to avoid DB pile-up");
        return;
      }

      this.syncInProgress = true;
      try {
        await this._syncPendingRecords();
      } finally {
        this.syncInProgress = false;
      }
    }, 60 * 1000);
  }

  start() {
    this.server.bind(this.port, this.host);
  }

  close() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }

    try {
      this.server.close();
    } catch (err) {
      if (err.code !== "ERR_SOCKET_DGRAM_NOT_RUNNING") {
        throw err;
      }
    }
  }
}

module.exports = UDPServer;
