/*
 * @(#) Connector.java	version 1.5   24/2/2018
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

import java.sql.ResultSet;

/**
 * Interface that defines all methods to be implemented by a JDBC Database connector.
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Modified by: Kostas Patroumpas, 12/6/2013; adjusted to TripleGeo functionality
 * Last modified: 24/2/2018
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
   * @param query  A SQL command for the SELECT query.
   * @return resultset with all results of the query.
   */
  public ResultSet executeQuery(String query);

  
  /**
   * Closes the connection to the database.
   */
  public void closeConnection();

}
