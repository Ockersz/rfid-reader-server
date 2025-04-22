// databaseManager.js
const mysql = require("mysql");

/**
 * DatabaseManager handles MySQL connection and query execution.
 */
class DatabaseManager {
  /**
   * @param {Object} config - MySQL connection configuration.
   */
  constructor(config) {
    this.config = config;
    this.connection = mysql.createConnection(config);
  }

  /**
   * Connects to the MySQL database.
   * @returns {Promise<void>}
   */
  connect() {
    return new Promise((resolve, reject) => {
      this.connection.connect((err) => {
        if (err) {
          console.error("Error connecting to the database:", err);
          return reject(err);
        }
        console.log("Connected to the database");
        resolve();
      });
    });
  }

  /**
   * Executes a SQL query.
   * @param {string} sql - The SQL query string.
   * @param {Array} values - The values to be escaped in the query.
   * @returns {Promise<any>} - Query results.
   */
  query(sql, values) {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, values, (err, results) => {
        if (err) {
          console.error("Error executing query:", err);
          return reject(err);
        }
        resolve(results);
      });
    });
  }
}

module.exports = DatabaseManager;
