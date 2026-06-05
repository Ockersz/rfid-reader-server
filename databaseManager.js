const mysql = require('mysql2/promise');

const DEFAULT_POOL_CONFIG = {
  waitForConnections: true,
  connectionLimit: 5,
  maxIdle: 0,
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
    this.activeConnectionWarnMs = options.activeConnectionWarnMs ?? 60_000;
    this.poolDebug = options.poolDebug ?? false;
    this.pool = mysql.createPool({
      ...config,
      ...DEFAULT_POOL_CONFIG,
      ...(options.pool || {}),
    });
    this._activeConnections = new Map();
    this._idleReaperInterval = null;

    this._setupPoolLogging();
    this._startIdleReaper();
  }

  _setupPoolLogging() {
    this.pool.on('connection', (connection) => {
      connection.once('end', () => {
        this._activeConnections.delete(connection.threadId);
      });

      connection.once('error', () => {
        this._activeConnections.delete(connection.threadId);
      });

      console.log(`🔌 [${this.name}] opened MySQL connection ${connection.threadId}`);
    });

    this.pool.on('acquire', (connection) => {
      this._activeConnections.set(connection.threadId, {
        acquiredAt: Date.now(),
        sql: null,
        lastWarnAt: null,
      });

      if (this.poolDebug) {
        console.log(`➡️ [${this.name}] acquired MySQL connection ${connection.threadId}`);
      }
    });

    this.pool.on('release', (connection) => {
      this._activeConnections.delete(connection.threadId);

      if (this.poolDebug) {
        console.log(`⬅️ [${this.name}] released MySQL connection ${connection.threadId}`);
      }
    });

    this.pool.on('enqueue', () => {
      console.warn(`⏳ [${this.name}] MySQL pool is full; query is waiting for a free connection`);
    });
  }

  _startIdleReaper() {
    const pool = this.pool.pool;
    const idleTimeout = pool?.config?.idleTimeout ?? DEFAULT_POOL_CONFIG.idleTimeout;

    this._idleReaperInterval = setInterval(() => {
      this._destroyExpiredIdleConnections();
      this._warnLongHeldConnections();
    }, Math.max(1_000, Math.min(idleTimeout, 10_000)));

    if (typeof this._idleReaperInterval.unref === 'function') {
      this._idleReaperInterval.unref();
    }
  }

  _destroyExpiredIdleConnections() {
    const pool = this.pool.pool;
    const freeConnections = pool?._freeConnections;

    if (!pool || !freeConnections || pool._closed) {
      return;
    }

    const maxIdle = pool.config?.maxIdle ?? DEFAULT_POOL_CONFIG.maxIdle;
    const idleTimeout = pool.config?.idleTimeout ?? DEFAULT_POOL_CONFIG.idleTimeout;
    const now = Date.now();

    while (freeConnections.length > 0) {
      const connection = freeConnections.get(0);
      const idleMs = now - (connection.lastActiveTime || now);
      const overMaxIdle = freeConnections.length > maxIdle;
      const expired = idleMs >= idleTimeout;

      if (!overMaxIdle && !expired) {
        break;
      }

      if (this.poolDebug || idleMs >= idleTimeout * 2) {
        console.warn(
          `🧹 [${this.name}] destroying idle MySQL connection ${connection.threadId} after ${idleMs}ms`,
          this._getPoolStats()
        );
      }

      connection.destroy();
    }
  }

  _setActiveQuery(connection, sql) {
    const threadId = connection?.connection?.threadId;

    if (!threadId || !this._activeConnections.has(threadId)) {
      return;
    }

    this._activeConnections.get(threadId).sql = this._formatSqlForLog(sql);
  }

  _warnLongHeldConnections() {
    const now = Date.now();

    for (const [threadId, connectionInfo] of this._activeConnections.entries()) {
      const heldMs = now - connectionInfo.acquiredAt;

      if (heldMs < this.activeConnectionWarnMs) {
        continue;
      }

      if (
        connectionInfo.lastWarnAt &&
        now - connectionInfo.lastWarnAt < this.activeConnectionWarnMs
      ) {
        continue;
      }

      connectionInfo.lastWarnAt = now;
      console.warn(
        `⚠️ [${this.name}] MySQL connection ${threadId} has been held for ${heldMs}ms`,
        this._getPoolStats(),
        connectionInfo.sql || 'no query recorded'
      );
    }
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
    let connection;

    try {
      connection = await this.pool.getConnection();
      this._setActiveQuery(connection, sql);

      const [rows] = await connection.execute(sql, values);
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
    } finally {
      if (connection) {
        connection.release();
      }
    }
  }

  /**
   * Closes the database connection pool.
   * @returns {Promise<void>}
   */
  async close() {
    try {
      if (this._idleReaperInterval) {
        clearInterval(this._idleReaperInterval);
        this._idleReaperInterval = null;
      }

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
