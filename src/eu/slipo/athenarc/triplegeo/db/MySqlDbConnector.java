/*
 * @(#) MySqlDbConnector.java 	version 1.9   24/2/2018
 *
 * Copyright (C) 2013-2019 Information Management Systems Institute, Athena R.C., Greece.
 *
 * This library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package eu.slipo.athenarc.triplegeo.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * MySQL implementation of DbConnector class.
 * NOTE: JDBC driver automatically uses the encoding specified by the host; no need to specify such parameter in the connection.
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY 
 * Created by: Kostas Patroumpas, 5/6/2013
 * Modified: 23/3/2017
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Last modified: 24/2/2018
 */
public class MySqlDbConnector implements DbConnector {

  private String host;
  private int port;
  private String dbName;
  private String username;
  private String password;
  private Connection connection;

  /** 
   * Constructor of DbConnector implementation class for establishing a connection to a MySQL database.
   * @param host  The IP of the machine where the database is hosted.
   * @param port  The port where the database is listening.
   * @param dbName  The name of the database to connect to.
   * @param username  The user name credential to access the database.
   * @param password  The password credential to access the database.
   */
  public MySqlDbConnector(String host, int port, String dbName, String username, String password) 
  {
    super();
    this.host = host;
    this.port = port;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.connection = openConnection();
  }

  /**
   * Returns the Database URL.
   *
   * @return databaseUrl with the URL of the MySQL database.
   */
  @Override
  public String getDatabaseUrl() 
  {
    return Constants.BASE_URL[Constants.MYSQL] + "//" + host + ":" + port + "/" + dbName;
  }


  /**
   * Returns the result of the SQL query executed against the MySQL database.
   *
   * @param query  A SQL command for the SELECT query.
   * @return Resultset with all results of the query.
   */
  @Override
  public ResultSet executeQuery(String query) 
  {
    ResultSet resultSet = null;
    try {
      Statement stmt = connection.createStatement();

      resultSet = stmt.executeQuery(query);

    } catch (SQLException e) {
    	ExceptionHandler.abort(e, "SQL query for data retrieval cannot be executed.");
    }
    return resultSet;
  }

  /**
   * Closes the connection to the MySQL database.
   */
  @Override
  public void closeConnection() 
  {
    try {
      connection.close();
      connection = null;
    } catch (SQLException ex) {
    	ExceptionHandler.abort(ex, "Cannot close connection to the database.");
    }
  }

  /**
   * Establishes a connection to the MySQL database.
   *
   * @return  Connection to the database.
   */
  private Connection openConnection() 
  {
    Connection connectionResult = null;
    try {
      Class.forName(Constants.DBMS_DRIVERS[Constants.MYSQL]);
      connectionResult = DriverManager.getConnection(getDatabaseUrl(), username, password);
      System.out.println("Connected to MySQL database!");
    } catch (Exception ex) {
    	ExceptionHandler.abort(ex, "Cannot connect to the database.");
    }
    return connectionResult;
  }

}
