/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tuberlin.cit.progressestimator.history.db

import java.sql.{ Connection, DriverManager }

import anorm.SqlParser._
import anorm._
/**
 * DB object.
 */
object DB {

  loadDriver("org.h2.Driver")
  /**
   * Silently tries to load a JDBC driver with the given `className`.
   *
   * @param className The FQName of the driver to be loaded.
   */
  private def loadDriver(className: String): Unit = try {
    Class.forName(className)
  } catch {
    case _: Throwable => // silently ignore exception
  }

  /**
   * Creates a database connection using the 'app.db.\$connName.conf' connection data.
   *
   * @param name The name of the connection configuration
   * @param config The config object holding the config data
   */
  def getConnection(url: String, user: String, pass: String): Connection = {
    println(s"[DB] connect to $url");
    val conn = DriverManager.getConnection(url, user, pass)
    conn
  }

  /**
   * Initialize the database schema.
   *
   * @param conn The DB connection.
   */
  def createSchema(conn: Connection): Unit = {
    createTableExecution(conn)
    createTableIteration(conn)
    createTableDstat(conn)
  }
  private def createTableExecution(conn: Connection): Unit = {
    if (!tableExists(conn, "execution")) {
      SQL(s"""
      CREATE TABLE execution (
        id              INTEGER        NOT NULL,
        name            VARCHAR(127)   NOT NULL,
        exp_name        VARCHAR(127)   NOT NULL     DEFAULT '',
        hash            CHAR(32)       NOT NULL,
        parameters      VARCHAR(511)   NOT NULL,
        input_file      VARCHAR(127)   NOT NULL,
        input_name      VARCHAR(127)   NOT NULL,
        input_size      LONG           NOT NULL,
        input_records   LONG           NOT NULL,
        time_start      TIMESTAMP      NOT NULL,
        time_finish     TIMESTAMP      NULL,
        PRIMARY KEY (id)
      )""").execute()(conn)
    }
  }
  private def createTableIteration(conn: Connection): Unit = {
    if (!tableExists(conn, "iteration")) {
      SQL(s"""
      CREATE TABLE iteration (
        app_id          INTEGER        NOT NULL,
        iteration_nr    INTEGER        NOT NULL,
        input_size      LONG           NOT NULL,
        input_records   LONG           NOT NULL,
        number_of_nodes INTEGER        NOT NULL,
        time_start      TIMESTAMP      NOT NULL,
        time_finish     TIMESTAMP      NULL,
        PRIMARY KEY (app_id, iteration_nr),
        FOREIGN KEY (app_id) REFERENCES execution(id) ON DELETE CASCADE
      )""").execute()(conn)
    }
  }
  private def createTableDstat(conn: Connection): Unit = {
    if (!tableExists(conn, "DstatEntry")) {
      SQL(s"""
      CREATE TABLE DstatEntry (
        appId          INTEGER        NOT NULL,
        time            TIMESTAMP      NOT NULL,
        host            VARCHAR(127)   NOT NULL,
        property        VARCHAR(31)    NOT NULL,
        value           DOUBLE         NOT NULL,
        PRIMARY KEY (appId, time, host, property),
        FOREIGN KEY (appId) REFERENCES execution(id) ON DELETE CASCADE
      )""").execute()(conn)
    }
  }
  private def tableExists(conn: Connection, tableName: String) = {
    val lcExists = conn.getMetaData.getTables(conn.getCatalog, null, tableName.toLowerCase, null).next()
    val ucExists = conn.getMetaData.getTables(conn.getCatalog, null, tableName.toUpperCase, null).next()
    ucExists || lcExists
  }

}
