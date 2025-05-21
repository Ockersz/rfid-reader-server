const mysql = require('mysql2/promise');

/**
 * DatabaseManager handles MySQL connection pooling and query execution using mysql2/promise.
 */
class DatabaseManager {
  /**
   * @param {Object} config - MySQL connection configuration.
   */
  constructor(config) {
    this.config = config;
    this.pool = mysql.createPool({
      ...config,
      waitForConnections: true,
      connectionLimit: 5,
      queueLimit: 0
    });
  }

  /**
   * Verifies a successful connection to the database.
   * @returns {Promise<void>}
   */
  async connect() {
    try {
      const connection = await this.pool.getConnection();
      await connection.ping();
      connection.release();
      console.log('✅ Connected to the database');
    } catch (err) {
      console.error('❌ Database connection failed:', err.message);
      throw err;
    }
  }

  /**
   * Executes a SQL query.
   * @param {string} sql - SQL query string.
   * @param {Array} values - Values to be safely injected.
   * @returns {Promise<any>} - Query results.
   */
  async query(sql, values = []) {
    try {
      const [rows] = await this.pool.execute(sql, values);
      return rows;
    } catch (err) {
      throw new Error(`Query failed: ${sql} - ${err.message}`);
    }
  }

  /**
   * Closes the database connection pool.
   * @returns {Promise<void>}
   */
  async close() {
    try {
      await this.pool.end();
      console.log('✅ Database pool closed');
    } catch (err) {
      console.error('❌ Error closing database pool:', err.message);
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
