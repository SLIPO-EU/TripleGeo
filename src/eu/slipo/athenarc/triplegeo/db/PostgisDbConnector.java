/*
 * @(#) PostgresqlDbConnector.java 	 version 1.8   24/2/2018
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
import java.util.Properties;

import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * PostgreSQL/PostGIS implementation of DbConnector class.
 * @author Kostas Patroumpas
 * @version 1.8
 */

/* DEVELOPMENT HISTORY 
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Modified by: Kostas Patroumpas, 24/5/2013; adjusted to TripleGeo functionality
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Modified: 13/12/2017; fixed issue with character encodings; verified that UTF characters are read and written correctly
 * Last modified: 24/2/2018
 */
public class PostgisDbConnector implements DbConnector {

  private String host;
  private int port;
  private String dbName;
  private String username;
  private String password;
  private String encoding;
  private Connection connection;

  /**
   * Constructor of DbConnector implementation class for establishing a connection to a PostgreSQL database with the PostGIS spatial extension.
   * @param host  The IP of the machine where the database is hosted.
   * @param port  The port where the database is listening.
   * @param dbName  The name of the database to connect to.
   * @param username  The user name credential to access the database.
   * @param password  The password credential to access the database.
   * @param encoding  The character encoding used in the database.
   */
  public PostgisDbConnector(String host, int port, String dbName, String username, String password, String encoding) 
  {
    super();
    this.host = host;
    this.port = port;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.encoding = encoding.toLowerCase();     //Values like "UTF-8", "ISO-8859-1", "ISO-8859-7"
    this.connection = openConnection();
  }

  /**
   * Returns the Database URL.
   *
   * @return databaseUrl with the URL of the PostgreSQL/PostGIS database.
   */
  @Override
  public String getDatabaseUrl() 
  {
    return Constants.BASE_URL[Constants.POSTGIS] + "//" + host + ":" + port + "/" + dbName;
  }


  /**
   * Returns the result of the SQL query executed against the PostgreSQL/PostGIS database.
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
   * Closes the connection to the PostgreSQL/PostGIS database.
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
   * Establishes a connection to the PostgreSQL/PostGIS database.
   *
   * @return  Connection to the database.
   */
  private Connection openConnection() 
  {
    Connection connectionResult = null;
    try {
      Class.forName(Constants.DBMS_DRIVERS[Constants.POSTGIS]);
      Properties props = new Properties();
      props.put("charSet", encoding);
      props.put("user", username);
      props.put("password", password);
      
      connectionResult = DriverManager.getConnection(getDatabaseUrl(), props);
      System.out.println("Connected to PostgreSQL/PostGIS database!");
    } catch (Exception ex) {
    	ExceptionHandler.abort(ex, "Cannot connect to the database.");
    }
    return connectionResult;
  }

}
