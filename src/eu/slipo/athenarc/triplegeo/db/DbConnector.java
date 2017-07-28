/*
 * @(#) Connector.java	version 1.2   16/3/2017
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

import java.sql.ResultSet;

/**
 * Interface that defines all methods to be implemented by a JDBC Database connector.
 *
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Modified by: Kostas Patroumpas, 12/6/2013
 */
public interface DbConnector {

  /**
   * Returns the Database URL.
   *
   * @return databaseUrl with the URL of the database.
   */
  public String getDatabaseUrl();

 
  /**
   * Returns the result of the SQL query executed against the database.
   *
   * @param query - String with the query.
   * @return resultset with the result of the query.
   */
  public ResultSet executeQuery(String query);

  /**
   * Closes database connection.
   */
  public void closeConnection();

}
