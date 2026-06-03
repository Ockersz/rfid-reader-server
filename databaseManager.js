const mysql = require('mysql2/promise');

const DEFAULT_POOL_CONFIG = {
  waitForConnections: true,
  connectionLimit: 5,
  maxIdle: 5,
  idleTimeout: 60_000,
  queueLimit: 100,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
};

/**
 * DatabaseManager handles MySQL connection pooling and query execution using mysql2/promise.
 */
class DatabaseManager {
  /**
   * @param {Object} config - MySQL connection configuration.
   * @param {Object} options - Pool and logging options.
   */
  constructor(config, options = {}) {
    this.config = config;
    this.name = options.name || config.database || config.host || 'mysql';
    this.slowQueryThresholdMs = options.slowQueryThresholdMs ?? 2_000;
    this.pool = mysql.createPool({
      ...config,
      ...DEFAULT_POOL_CONFIG,
      ...(options.pool || {}),
    });

    this._setupPoolLogging();
  }

  _setupPoolLogging() {
    this.pool.on('connection', (connection) => {
      console.log(`🔌 [${this.name}] opened MySQL connection ${connection.threadId}`);
    });

    this.pool.on('acquire', (connection) => {
      console.log(`➡️ [${this.name}] acquired MySQL connection ${connection.threadId}`);
    });

    this.pool.on('release', (connection) => {
      console.log(`⬅️ [${this.name}] released MySQL connection ${connection.threadId}`);
    });

    this.pool.on('enqueue', () => {
      console.warn(`⏳ [${this.name}] MySQL pool is full; query is waiting for a free connection`);
    });
  }

  _getPoolStats() {
    const pool = this.pool.pool;

    return {
      total: pool?._allConnections?.length ?? 0,
      free: pool?._freeConnections?.length ?? 0,
      queued: pool?._connectionQueue?.length ?? 0,
      limit: pool?.config?.connectionLimit,
    };
  }

  _formatSqlForLog(sql) {
    return String(sql).replace(/\s+/g, ' ').trim().slice(0, 500);
  }

  /**
   * Verifies a successful connection to the database.
   * @returns {Promise<void>}
   */
  async connect() {
    let connection;

    try {
      connection = await this.pool.getConnection();
      await connection.ping();
      console.log(`✅ [${this.name}] Connected to the database`);
    } catch (err) {
      console.error(`❌ [${this.name}] Database connection failed:`, err.message);
      throw err;
    } finally {
      if (connection) {
        connection.release();
      }
    }
  }

  /**
   * Executes a SQL query.
   * @param {string} sql - SQL query string.
   * @param {Array} values - Values to be safely injected.
   * @returns {Promise<any>} - Query results.
   */
  async query(sql, values = []) {
    const startTime = Date.now();

    try {
      const [rows] = await this.pool.execute(sql, values);
      const durationMs = Date.now() - startTime;

      if (durationMs >= this.slowQueryThresholdMs) {
        console.warn(
          `🐢 [${this.name}] slow query took ${durationMs}ms`,
          this._getPoolStats(),
          this._formatSqlForLog(sql)
        );
      }

      return rows;
    } catch (err) {
      const durationMs = Date.now() - startTime;
      throw new Error(
        `Query failed after ${durationMs}ms on ${this.name}: ${this._formatSqlForLog(sql)} - ${err.message}`
      );
    }
  }

  /**
   * Closes the database connection pool.
   * @returns {Promise<void>}
   */
  async close() {
    try {
      await this.pool.end();
      console.log(`✅ [${this.name}] Database pool closed`);
    } catch (err) {
      console.error(`❌ [${this.name}] Error closing database pool:`, err.message);
      throw err;
    }
  }

  /**
   * Escapes a value for use in raw queries.
   * @param {*} value - The value to escape.
   * @returns {string}
   */
  escape(value) {
    return mysql.escape(value);
  }
}

module.exports = DatabaseManager;
