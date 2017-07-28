/*
 * @(#) PostgresqlDbConnector.java 	 version 1.2   24/5/2013
 *
 * Copyright (C) 2013-2017 Institute for the Management of Information Systems, Athena RC, Greece.
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

/**
 * PostgreSQL implementation of DbConnector class.
 * 
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Modified by: Kostas Patroumpas, 24/3/2017
 */
public class PostgisDbConnector implements DbConnector {

  private String host;
  private int port;
  private String dbName;
  private String username;
  private String password;
  private Connection connection;

  /*
   * Constructs a DbConnector Object.
   *
   * @param host - String with the IP where the database is hosted.
   * @param port - int with the port where the database is listening.
   * @param dbName - String with the name of the database.
   * @param username - String with the user name to access the database.
   * @param password - String with the password to access the database.
   */
  public PostgisDbConnector(String host, int port, String dbName,
                               String username, String password) {
    super();
    this.host = host;
    this.port = port;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.connection = openConnection();
  }

  @Override
  public String getDatabaseUrl() {
    return Constants.BASE_URL[Constants.POSTGIS] + "//" + host + ":" + port + "/" + dbName;
  }


  @Override
  public ResultSet executeQuery(String query) {
    ResultSet resultSet = null;
    try {
      Statement stmt = connection.createStatement();

      resultSet = stmt.executeQuery(query);

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return resultSet;
  }

  @Override
  public void closeConnection() {
    try {
      connection.close();
      connection = null;
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Returns a connection to the Database.
   *
   * @return connection to the database.
   */
  private Connection openConnection() {
    Connection connectionResult = null;
    try {
      Class.forName(Constants.DRIVERS[Constants.POSTGIS]);
      connectionResult = DriverManager.getConnection(
              getDatabaseUrl(), username, password);
      System.out.println("Connected to PostgreSQL/PostGIS database!");
    } catch (Exception ex) {
      //throw new SQLException ();
      ex.printStackTrace();
    }
    return connectionResult;
  }

}
