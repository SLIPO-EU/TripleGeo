/*
 * @(#) SpatiaLiteDbConnector.java 	 version 1.8   24/2/2018
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

import org.sqlite.SQLiteConfig;

import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * SpatiaLite implementation of DbConnector class. Used in establishing a connection to a SpatiaLite database. 
 * IMPORTANT! Native libraries for SpatiaLite (https://www.gaia-gis.it/fossil/libspatialite/index) must be located in the directory at which the Java application is called in order to run smoothly this class.
 * @author Kostas Patroumpas
 * @version 1.8
 */

/* DEVELOPMENT HISTORY 
 * Created by: Kostas Patroumpas, 25/8/2017
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Modified: 13/12/2017; fixed issue with character encodings
 * Last modified: 24/2/2018
 */
public class SpatiaLiteDbConnector implements DbConnector {

  private String dbName;
  private Connection connection;
  private String encoding;

  /**
   * Constructor of DbConnector implementation class for establishing connection to a SpatiaLite database.
   * @param dbName  The name of the database.
   * @param encoding  The character encoding used in the database.
   */
  public SpatiaLiteDbConnector(String dbName, String encoding) 
  {
    super();
    this.dbName = dbName;
    this.encoding = encoding;
    this.connection = openConnection();
  }

  /**
   * Returns the Database URL.
   *
   * @return databaseUrl with the URL of the database.
   */
  @Override
  public String getDatabaseUrl() 
  {	  
    return Constants.BASE_URL[Constants.SPATIALITE] + dbName;
  }

  /**
   * Returns the result of the SQL query executed against the SpatiaLite database.
   *
   * @param query  A SQL command for the SELECT query.
   * @return  Resultset with all results of the query.
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
   * Closes the connection to the database.
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
   * Establishes a connection to the Database.
   *
   * @return  Connection to the database.
   */
  private Connection openConnection() 
  {
    Connection connectionResult = null;
    try {
      Class.forName(Constants.DBMS_DRIVERS[Constants.SPATIALITE]);
  
      // Enabling dynamic extension loading -- absolutely required by SpatiaLite
      SQLiteConfig config = new SQLiteConfig();
      config.enableLoadExtension(true);
      config.setEncoding(SQLiteConfig.Encoding.valueOf(encoding));
      
      connectionResult = DriverManager.getConnection(getDatabaseUrl(), config.toProperties());
      
      Statement stmt = connectionResult.createStatement();
      stmt.setQueryTimeout(30); // set timeout to 30 sec

	  //IMPORTANT: loading SpatiaLite
      stmt.execute("SELECT load_extension('mod_spatialite')"); 
      
      String sql = "SELECT sqlite_version(), spatialite_version()";
   	  ResultSet rs = stmt.executeQuery( sql );
   	  while (rs.next()) {
	        String msg = "Connected to SQLite ver. ";
   	        msg += rs.getString(1);
  			msg += " with extension SpatiaLite ver. ";
   		    msg += rs.getString(2) + ".";
   		    System.out.println( msg );
		}
    } catch (Exception ex) { 
    	ExceptionHandler.abort(ex, "Cannot connect to the database. SpatiaLite module is missing! Native libraries for SpatiaLite (https://www.gaia-gis.it/fossil/libspatialite/index) must be located in the directory at which the Java application is called in order to run smoothly this class.");
    }
    return connectionResult;
  }

}
