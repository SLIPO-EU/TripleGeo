/*
 * @(#) UtilsConstants.java 	 version 1.3   28/11/2017
 *
 * Copyright (C) 2013-2017 Information Systems Management Institute, Athena R.C., Greece.
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
package eu.slipo.athenarc.triplegeo.utils;

/**
 * Constants for the utils library.
 *
 * @author jonathangsc
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * @version 2nd February 2012.
 * Modified by: Kostas Patroumpas, 28/11/2017
 */
public class Constants {

  // Replacement values
  public static final String WGS84 = "wgs84/";
  public static final String LINE_SEPARATOR = "\n";
  public static final String UTF_8 = "UTF-8";
  public static final String FEAT = "Geom_";
  
  public static final String STRING_TO_REPLACE = "+";
  public static final String REPLACEMENT = "%20";
  public static final String SEPARATOR = "_";
  public static final String BLANK = " ";

  // DB Types
  public static final int MSACCESS = 0;
  public static final int MYSQL = 1;
  public static final int ORACLE = 2;
  public static final int POSTGIS = 3;
  public static final int DB2 = 4;
  public static final int SQLSERVER = 5;
  public static final int SPATIALITE = 6;

  // DB Drivers
  public static final String[] DRIVERS =
    {"sun.jdbc.odbc.JdbcOdbcDriver", "com.mysql.jdbc.Driver", "oracle.jdbc.driver.OracleDriver", 
     "org.postgresql.Driver", "com.ibm.db2.jcc.DB2Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "org.sqlite.JDBC"};

  //public static final String[] DBMS = {"MSACCESS", "MYSQL", "ORACLE", "POSTGIS", "DB2", "MSSQLSERVER", "SPATIALITE"};
  public static final String[] BASE_URL = {"jdbc:odbc:", "jdbc:mysql:", "jdbc:oracle:thin:", "jdbc:postgresql:", "jdbc:db2:", "jdbc:sqlserver:", "jdbc:sqlite:"};

	
  //Alias for most common namespaces
  public static final String NS_GEO = "http://www.opengis.net/ont/geosparql#";
  public static final String NS_SF =  "http://www.opengis.net/ont/sf#";
  public static final String NS_GML = "http://loki.cae.drexel.edu/~wbs/ontology/2004/09/ogc-gml#";
  public static final String NS_XSD = "http://www.w3.org/2001/XMLSchema#";
  public static final String NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  public static final String NS_POS = "http://www.w3.org/2003/01/geo/wgs84_pos#";
  public static final String NS_VIRT = "http://www.openlinksw.com/schemas/virtrdf#";
  public static final String NS_DC = "http://purl.org/dc/terms/";
  
  
  //alias for most common tags
  public static final String GEOMETRY = "Geometry";
  public static final String FEATURE = "Feature";
  public static final String LINE_STRING = "LineString";
  public static final String MULTI_LINE_STRING = "MultiLineString";
  public static final String POLYGON = "Polygon";
  public static final String MULTI_POLYGON = "MultiPolygon";
  public static final String POINT = "Point";
  public static final String LATITUDE = "lat";
  public static final String LONGITUDE = "long";
  public static final String GML = "gml";
  public static final String WKT = "asWKT";
  public static final String WKTLiteral = "wktLiteral";
  public static final String NAME = "name";
  public static final String TYPE = "type";
  
  public static final String COPYRIGHT = "*********************************************************************\n*                      TripleGeo version 1.3                        *\n* Developed by the Information Systems Management Institute.        *\n* Copyright (C) 2013-2017 Athena Research Center, Greece.           *\n* This program comes with ABSOLUTELY NO WARRANTY.                   *\n* This is FREE software, distributed under GPL license.             *\n* You are welcome to redistribute it under certain conditions       *\n* as mentioned in the accompanying LICENSE file.                    *\n*********************************************************************\n";
  public static final String RML_COPYRIGHT = "NOTICE: TripleGeo makes use of RML processing modules (http://rml.io/) under MIT license; Copyright (c) 2013-2017, Ghent University-iMinds-Multimedia Lab.";
  public static final String OSMPOISPBF_COPYRIGHT = "NOTICE: TripleGeo employs OSM filters specified by the OsmPoisPbf tool (https://github.com/MorbZ/OsmPoisPbf) under GPL license; Copyright (c) 2012-2015, Merten Peetz.";
  public static final String INCORRECT_CONFIG = "Incorrect number of arguments. A properties file with proper configuration settings is required.";
  public static final String INCORRECT_SETTING = "Incorrect or no value set for at least one parameter. Please specify a correct value in the configuration settings.";
  public static final String INCORRECT_DBMS = "Incorrect or no value set for the DBMS where input data is stored. Please specify a correct value in the configuration settings.";
  public static final String NO_REPROJECTION = "No reprojection in another coordinate reference system will take place.\nInput data is expected in WGS84 reference system.";
  
}
