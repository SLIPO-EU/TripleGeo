/*
 * @(#) RdbToRdf.java 	 version 1.6   27/2/2018
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
package eu.slipo.athenarc.triplegeo.tools;

import java.sql.ResultSet;

import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import eu.slipo.athenarc.triplegeo.db.DB2DbConnector;
import eu.slipo.athenarc.triplegeo.db.DbConnector;
import eu.slipo.athenarc.triplegeo.db.MsAccessDbConnector;
import eu.slipo.athenarc.triplegeo.db.MySqlDbConnector;
import eu.slipo.athenarc.triplegeo.db.OracleDbConnector;
import eu.slipo.athenarc.triplegeo.db.PostgisDbConnector;
import eu.slipo.athenarc.triplegeo.db.SqlServerDbConnector;
import eu.slipo.athenarc.triplegeo.db.SpatiaLiteDbConnector;

import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;
import eu.slipo.athenarc.triplegeo.utils.Constants;

/**
 * Entry point of the utility for extracting RDF triples from spatially-enabled DBMSs.
 * @author Kostas Patroumpas
 * @version 1.6
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 8/2/2013
 * Modified: 10/2/2013, added support for Oracle Spatial and PostGIS databases
 * Modified: 6/3/2013, added support for transformation from a source CRS to a target CRS
 * Modified: 15/3/2013, added support for exporting custom geometries to (1) Virtuoso RDF and (2) according to WGS84 GeoPositioning RDF vocabulary 
 * Modified: 6/6/2013, added support for IBM DB2 and MySQL databases
 * Modified: 9/3/2017, added support for transformation from a source CRS to a target CRS (i.e., change georeference system)
 * Modified: 16/3/2017, added support for serializations in a streaming fashion
 * Modified: 10/4/2017, added support for Microsoft SQL Server Spatial databases
 * Modified: 25/8/2017, added support for SpatiaLite extension of SQLite databases
 * Modified: 27/9/2017, added support for RML mappings for multiple thematic attributes
 * Modified: 31/10/2017, added support for transforming non-spatial tables using RML mappings
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 13/11/2017, added support for recognizing a classification scheme (RML mode only)
 * Modified: 24/11/2017, added support for recognizing character encoding for strings
 * Modified: 11/12/2017, added support on UTF-8 encoding in the result of RML conversion.
 * Modified: 14/12/2017, added support for ESRI personal geodatabases (Microsoft Access .mdb format). CAUTION: Include -Dfile.encoding=UTF-8 when applying against geodatabases with UTF-8 encoding.
 * Last modified by: Kostas Patroumpas, 27/2/3018
 */
public class RdbToRdf {

  Converter myConverter;
  Assistant myAssistant;
  private MathTransform reproject = null;
  int sourceSRID;                         //Source CRS according to EPSG 
  int targetSRID;                         //Target CRS according to EPSG
  DbConnector databaseConnector = null;   //Instantiation of Connector class to a DBMS
  static Configuration currentConfig;     //User-specified configuration settings
  static Classification classification;   //Classification hierarchy for assigning categories to features
  static String outputFile;               //Output RDF file
  
  //Initialize a CRS factory for possible reprojections
  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
	       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
 
  
  /**
   * Constructor for the transformation process from a spatially-enabled DBMS to RDF.
   * @param config  Parameters to configure the transformation.
   * @param classific  Instantiation of the classification scheme that assigns categories to input features.
   * @param outFile  Path to the output file that collects RDF triples.
   * @param sourceSRID  Spatial reference system (EPSG code) of the input geometries.
   * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
   */
  public RdbToRdf(Configuration config, Classification classific, String outFile, int sourceSRID, int targetSRID) {
	  
	  currentConfig = config;
	  classification = classific;
	  outputFile = outFile;
      this.sourceSRID = sourceSRID;
      this.targetSRID = targetSRID;
      myAssistant = new Assistant();
	  
      try
      {
	      //Determine connection type to the specified DBMS
	      switch(currentConfig.dbType.toUpperCase()) {
	        case "MSACCESS":
	          databaseConnector = new MsAccessDbConnector(
	        		  currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "MYSQL":
	          databaseConnector = new MySqlDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "ORACLE":
	          databaseConnector = new OracleDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword, currentConfig.encoding);
	          break;
	        case "POSTGIS":
	          databaseConnector = new PostgisDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword, currentConfig.encoding);
	          break;
	        case "DB2":
	           databaseConnector = new DB2DbConnector(
	        		   currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword, currentConfig.encoding);
	          break;
	        case "SQLSERVER":
	            databaseConnector = new SqlServerDbConnector(
	         		   currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	           break;  
	        case "SPATIALITE":
	            databaseConnector = new SpatiaLiteDbConnector(currentConfig.dbName, currentConfig.encoding);
	           break; 
	        default:
	        	throw new IllegalArgumentException(Constants.INCORRECT_DBMS);
	      }
      } catch (Exception e) {
    	  ExceptionHandler.abort(e, Constants.INCORRECT_DBMS);      //Execution terminated abnormally
      }

      //Check if a coordinate transform is required for geometries
      if (currentConfig.targetCRS != null)
  	    try {
  	        boolean lenient = true; // allow for some error due to different datums
  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
  	        reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
  	        
  		} catch (Exception e) {
  			ExceptionHandler.abort(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
  		}
      
      // Other parameters
      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
    	  currentConfig.defaultLang = "en";
      }
	  
  }

  
   /**
	* Applies transformation according to the configuration settings.
	*/
	public void apply() 
	{ 
		try { 
			  //Collect results from the database
			  ResultSet rs = collectData(databaseConnector);
			  
			  if (currentConfig.mode.contains("GRAPH"))
			  {
			      //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig, outputFile);
			  
				  //Export data after constructing a model on disk
				  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
			  
				  //Remove all temporary files as soon as processing is finished
				  myAssistant.removeDirectory(myConverter.getTDBDir());
			  }
			  else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig, outputFile);
				  
				  //Export data in a streaming fashion
				  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
				}
			  else if (currentConfig.mode.contains("RML"))
				{
				  //Mode RML: consume records and apply RML mappings in order to get triples
				  myConverter =  new RMLConverter(currentConfig);
				  
				  //Export data in a streaming fashion according to RML mappings
				  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
				}
			} catch (Exception e) {
				ExceptionHandler.abort(e, "");
	  		}
		
	}


	/**
	 * Connects to a database and retrieves records from a table (including geometric and non-spatial attributes) according to an SQL query.
	 * @param dbConn  Database connector object to a DBMS.
	 * @return A resultset with all results to the SQL query.
	 * @throws Exception
	 */
     private ResultSet collectData(DbConnector dbConn) throws Exception {
	
	    int totalRows;

	    //System.out.println(myAssistant.getGMTime() + " Started retrieving features from the database...");

	    String condition = "";
	    
	    //Check if criteria have been specified for selection of qualifying records
	    if (currentConfig.filterSQLCondition == null)
	    	condition = "";       //All table contents will be exported
	    else
	    	condition = " WHERE ( " + currentConfig.filterSQLCondition + " )";
	    
	    //Count records
	    String sql = "SELECT count(*) AS total FROM " +  currentConfig.tableName + condition;
	    	    
	    ResultSet rs = dbConn.executeQuery(sql);
	    rs.next();
	    totalRows = rs.getInt("total");    //total records to be exported
	    System.out.println(myAssistant.getGMTime() + " Number of database records to be processed: " + totalRows);
	  
	    //In case no CRS transformation has been specified, assume georeferencing in WGS84
	    //TODO: Detect CRS directly from the database (if supported by the DBMS)
	    //E.g., in PostGIS: "SELECT srid FROM geometry_columns WHERE f_table_name = 'osm_pois12';"
    	if (sourceSRID == 0)
    	{
    		sourceSRID = targetSRID = 4326;
    		System.out.println(Constants.WGS84_PROJECTION);
    	}
    	//System.out.println("sourceSRID=" + sourceSRID + " targetSRID=" + targetSRID);
    	
	    //Initialize SQL statement to be used for retrieval
	    sql = " SELECT " + currentConfig.tableName + ".*";    //Retrieve all attributes
	    
	    //Formulate geometry retrieval according to the spatial syntax of each DBMS, also checking whether spatial transformation is needed
	    //Geometry is returned as a WKT string (after reprojection, if specified)
	    if (currentConfig.attrGeometry != null)
	    {
	      if (dbConn.getClass().getSimpleName().contains("Access"))
	      {  
	    	  //MS Access (Personal ESRI geodatabase)
	    	  myAssistant.initPGDBDecoder();                 //Initialize decoder for geometries in a personal geodatabase
	      }
	      else if (dbConn.getClass().getSimpleName().contains("Oracle"))
	      {
	    	  //ORACLE
	    	  if (sourceSRID != targetSRID)
	    		  sql += ", SDO_UTIL.TO_WKTGEOMETRY(SDO_CS.TRANSFORM(" + currentConfig.attrGeometry + ", " + targetSRID + ")) WktGeometry";   
	    	  else
	    		  sql += ", SDO_UTIL.TO_WKTGEOMETRY(" + currentConfig.attrGeometry + ") WktGeometry";
	      }
	      else if (dbConn.getClass().getSimpleName().contains("Postgis"))
	      {    
	    	  //PostGIS
	    	  if (sourceSRID != targetSRID)
	    		  sql += ", ST_AsText(ST_Transform(" + currentConfig.attrGeometry + ", " + targetSRID + ")) WktGeometry"; 
	    	  else
	    		  sql += ", ST_AsText(" + currentConfig.attrGeometry + ") WktGeometry"; 
	      }
	      else if (dbConn.getClass().getSimpleName().contains("MySql"))
	      {    
	    	  //MySQL --  As of version 5.7, no CRS transformation is possible for geometries.
	    	  sql += ", AsText(" + currentConfig.attrGeometry + ") WktGeometry";
	      }
	      else if (dbConn.getClass().getSimpleName().contains("DB2"))
	      {    
	    	  //IBM DB2
	    	  if (sourceSRID != targetSRID)
	    		  sql += ", db2gse.ST_AsText(db2gse.ST_Transform(" + currentConfig.attrGeometry + ", " + targetSRID + ")) WktGeometry";
	    	  else
	    		  sql += ", db2gse.ST_AsText(" + currentConfig.attrGeometry + ") WktGeometry";
	      }
	      else if (dbConn.getClass().getSimpleName().contains("SqlServer"))
	      {    
	    	  //Microsoft SQL Server --  As of version 13 (SQL Server 2016), no CRS transformation is possible for geography attributes.
	    	  sql += ", " + currentConfig.attrGeometry + ".STAsText() WktGeometry";
	      }
	      else if (dbConn.getClass().getSimpleName().contains("SpatiaLite"))
	      {    
	    	  //SpatiaLite
	    	  if (sourceSRID != targetSRID)
	    		  sql += ", ST_AsText(ST_Transform(" + currentConfig.attrGeometry + ", " + targetSRID + ")) WktGeometry";
	    	  else
	    		  sql += ", ST_AsText(" + currentConfig.attrGeometry + ") WktGeometry";
	      }
	  }
	    
      //Append the rest of the SQL statement
      sql += " FROM " + currentConfig.tableName + condition;	      
      //System.out.println(sql);
      
      //Execute SQL query in the DBMS and fetch all results
      rs = dbConn.executeQuery(sql);
      
      return rs;                      //Report records retrieved from the database       
   }

}
