/*
 * @(#) MsAccessDbConnector.java 	version 1.6   24/2/2018
 *
 * Copyright (C) 2013-2018 Information Systems Management Institute, Athena R.C., Greece.
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
 * Microsoft Access implementation of DbConnector class.
 * CAUTION: In the execution, include -Dfile.encoding=UTF-8 when applying against geodatabases with UTF-8 encoding; similarly for other encodings.
 * @author Kostas Patroumpas
 * @version 1.6
 */

/* DEVELOPMENT HISTORY 
 * Modified by: Kostas Patroumpas, 23/3/2017
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Modified: 14/12/2017; using uCanAccess library for connections
 * Last modified: 24/2/2018
 */
public class MsAccessDbConnector implements DbConnector {

  private String dbName;
  private String username;
  private String password;
  private Connection connection;

  /**
   * Constructor of DbConnector implementation class for establishing a connection to a MSAccess database.
   * @param dbName  Full path to the MSAccess database.
   * @param username  The user name to access the database (if applicable).
   * @param password  The password to access the database (if applicable).
   */
  public MsAccessDbConnector(String dbName, String username, String password) 
  {
    super();
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.connection = openConnection();
  }

  /**
   * Returns the Database URL.
   *
   * @return databaseUrl with the URL of the MSAccess database.
   */
  @Override
  public String getDatabaseUrl() 
  {
    return Constants.BASE_URL[Constants.MSACCESS] + "//" + dbName;
  }


  /**
   * Returns the result of the SQL query executed against the MSAccess database.
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
   * Closes the connection to the MSAccess database.
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
   * Establishes a connection to the MSAccess database.
   *
   * @return  Connection to the database.
   */
  private Connection openConnection() 
  {
    Connection connectionResult = null;
    try {      
      connectionResult = DriverManager.getConnection(getDatabaseUrl(), username, password);
      System.out.println("Connected to Microsoft Access database!");
    } catch (Exception ex) {
    	ExceptionHandler.abort(ex, "Cannot connect to the database.");
    }
    return connectionResult;
  }

}
