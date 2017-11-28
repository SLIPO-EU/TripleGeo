/*
 * @(#) RdbToRdf.java 	 version 1.3   27/11/2017
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
package eu.slipo.athenarc.triplegeo.tools;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.model.dataset.SimpleRMLDataset;
//import be.ugent.mmlab.rml.model.dataset.StdRMLDataset;
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
 * Entry point of the utility for extracting RDF triples from geospatially-enabled DBMSs.
 * @author Kostas Patroumpas
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
 * Last modified by: Kostas Patroumpas, 27/11/2017
 */
public class RdbToRdf {

  Converter myConverter;
  Assistant myAssistant;
  public int sourceSRID;                  //Source CRS according to EPSG 
  public int targetSRID;                  //Target CRS according to EPSG
  DbConnector databaseConnector = null;
  static Configuration currentConfig;     //User-specified configuration settings
  static Classification classification;   //Classification hierarchy for assigning categories to features
  static String outputFile;               //Output RDF file

  
  //Class constructor
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
	        case "MSACCESS":   //Currently NOT supported
	          databaseConnector = new MsAccessDbConnector(
	        		  currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "MYSQL":
	          databaseConnector = new MySqlDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "ORACLE":
	          databaseConnector = new OracleDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "POSTGIS":
	          databaseConnector = new PostgisDbConnector(
	        		  currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "DB2":
	           databaseConnector = new DB2DbConnector(
	        		   currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	          break;
	        case "SQLSERVER":
	            databaseConnector = new SqlServerDbConnector(
	         		   currentConfig.dbHost, currentConfig.dbPort, currentConfig.dbName, currentConfig.dbUserName, currentConfig.dbPassword);
	           break;  
	        case "SPATIALITE":
	            databaseConnector = new SpatiaLiteDbConnector(currentConfig.dbName);
	           break; 
	        default:
	        	throw new IllegalArgumentException(Constants.INCORRECT_DBMS);
	      }
      } catch (Exception e) {
    	  ExceptionHandler.invoke(e, Constants.INCORRECT_DBMS);      //Execution terminated abnormally
      }

      // Other parameters
      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
    	  currentConfig.defaultLang = "en";
      }
	  
  }

   /*
	* Apply transformation according to configuration settings.
	*/
	public void apply() 
	{ 
		try { 
			  //Collect results from the database
			  ResultSet rs = collectData(databaseConnector);
			  
			  if (currentConfig.mode.contains("GRAPH"))
			  {
			      //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig);
			  
				  //Export data after constructing a model on disk
				  executeParser4Graph(rs);
			  
				  //Remove all temporary files as soon as processing is finished
				  myAssistant.removeDirectory(currentConfig.tmpDir);
			  }
			  else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4Stream(rs);
				}
			  else if (currentConfig.mode.contains("RML"))
				{
				  //Mode RML: consume records and apply RML mappings in order to get triples
				  myConverter =  new RMLConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4RML(rs);
				}
			} catch (Exception e) {
				ExceptionHandler.invoke(e, "");
	  		}
		
	}

  /**
   * 
   * Connect to a database and get records from a table (including geometric and non-spatial attributes)
   */
private ResultSet collectData(DbConnector dbConn) throws Exception {
	
	    int totalRows;

	    //System.out.println(myAssistant.getGMTime() + " Started retrieving features from the database...");

	    String condition = "";
/*	     
	    //Identify additional attributes for name and type of each feature
	    String auxColumns = "";
	    if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
	    	auxColumns = currentConfig.attrCategory + ", " + currentConfig.attrName + ", ";
*/	    
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
	  
	    //Initialize SQL statement to be used for retrieval
	    sql = " SELECT " + currentConfig.tableName + ".*"; // + currentConfig.attrKey + ", " + auxColumns + " ";
	    
	    //Formulate geometry retrieval according to the spatial syntax of each DBMS, also checking whether spatial transformation is needed
	    if (currentConfig.attrGeometry != null)
	    {
	      if (dbConn.getClass().getSimpleName().contains("Oracle"))
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

  /**
   * 
   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
   */
  private void executeParser4Graph(ResultSet rs) throws Exception {
  
	  long t_start = System.currentTimeMillis();
	  long dt = 0;
	    
	  int numRec = 0;
	    
      //Iterate through all records
      while (rs.next()) 
      {
        String wkt = rs.getString("WktGeometry");

        //Parse geometric representation (including encoding to the target CRS)
        myConverter.parseGeom2RDF(currentConfig.featureName, rs.getString(currentConfig.attrKey), wkt, targetSRID);
        
        //Process non-spatial attributes for name and type
        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.getString(currentConfig.attrKey), rs.getString(currentConfig.attrName), rs.getString(currentConfig.attrCategory));
        
        myAssistant.notifyProgress(++numRec);
        
      }

    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    System.out.println(myAssistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
    System.out.println(myAssistant.getGMTime() + " Starting to write triples to file...");
    
    //Count the number of statements
    int numStmt = myConverter.getModel().listStatements().toSet().size();
    
    //Export model to a suitable format
    FileOutputStream out = new FileOutputStream(outputFile);
    myConverter.getModel().write(out, currentConfig.serialization);  
    
    //Final notification
    dt = System.currentTimeMillis() - t_start;
    myAssistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, outputFile);
  }


  /**
   * 
   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
   */
  private void executeParser4Stream(ResultSet rs) throws Exception {
  
	  long t_start = System.currentTimeMillis();
	  long dt = 0;
	    
	  int numRec = 0;
	  int numTriples = 0;
	    
	  OutputStream outFile = null;
	  try {
			outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
	  } 
	  catch (FileNotFoundException e) {
		  ExceptionHandler.invoke(e, "Output file not specified correctly.");
	  } 
		
	  //CAUTION! Hard constraint: serialization into N-TRIPLES is only supported by Jena riot (stream) interface
	  StreamRDF stream = StreamRDFWriter.getWriterStream(outFile, RDFFormat.NTRIPLES);   //StreamRDFWriter.getWriterStream(os, Lang.NT);
		
	  stream.start();             //Start issuing streaming triples
	    
      //Iterate through all records
      while (rs.next()) 
      {
    	  	myConverter =  new StreamConverter(currentConfig);
	        String wkt = rs.getString("WktGeometry");
	
	        //Parse geometric representation (including encoding to the target CRS)
	        myConverter.parseGeom2RDF(currentConfig.featureName, rs.getString(currentConfig.attrKey), wkt, targetSRID);
	        
	        //Process non-spatial attributes for name and type
	        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
	        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.getString(currentConfig.attrKey), rs.getString(currentConfig.attrName), rs.getString(currentConfig.attrCategory));
	        
			//Append each triple to the output stream 
			for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
				stream.triple(myConverter.getTriples().get(i));
				numTriples++;
			}
			
			myAssistant.notifyProgress(++numRec);
      }

	stream.finish();               //Finished issuing triples
	
    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
    
  }


  
  /**
   * 
   * Mode: RML -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping
   */
  private void executeParser4RML(ResultSet rs) {

    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
    long t_start = System.currentTimeMillis();
    long dt = 0;
   
    int numRec = 0;
    int numTriples = 0;

//    RMLDataset dataset = new StdRMLDataset();
    RMLDataset dataset = new SimpleRMLDataset();
    
    //Determine the serialization to be applied for the output triples
    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
    	  
	try {
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		
		//Iterate through all records
	    while (rs.next()) 
	    {
	      	//Pass all NOT NULL attribute values into a hash map in order to apply RML mapping(s) directly
	      	HashMap<String, String> row = new HashMap<>();	
	      	for (int i=1; i<=rs.getMetaData().getColumnCount(); i++)
	      	{
	      		if ((!rs.getMetaData().getColumnLabel(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (rs.getString(i) != null))
	      		{
	      		    //Names of attributes in upper case; case-sensitive in RML mappings!
	      			row.put(rs.getMetaData().getColumnLabel(i).toUpperCase(), myAssistant.encodeUTF(rs.getString(i), currentConfig.encoding));
	      			//System.out.print(rs.getMetaData().getColumnLabel(i) + ":" + rs.getString(i) + " ");
	      		}
	      	}
	      	
	      	//Include a category identifier, as found in the classification scheme
	      	if (classification != null)
	      		row.put("CATEGORY_URI", classification.getURI(rs.getString(currentConfig.attrCategory)));
	      	//System.out.println(classification.getURI(rs.getString(currentConfig.attrCategory)));
	      	
	      	//Include geometry attribute, if specified
	      	if (currentConfig.attrGeometry != null)
	      	{
			    String wkt = rs.getString("WktGeometry");
			    //System.out.println(wkt);
		        row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Update attribute for the geometry as WKT along with the CRS
	      	}
	        //System.out.println("");	      	
	        //System.out.println(row.toString());
	      	
	        //Apply the transformation according to the given RML mapping		      
	        myConverter.parseWithRML(row, dataset);
	       
	        myAssistant.notifyProgress(++numRec);
		  
		    //Periodically, dump results into output file
			if (numRec % 10 == 0) 
			{
				numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
				dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//				dataset = new StdRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
			}
		}	
		
		//Dump any pending results into output file
		numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
		writer.flush();
		writer.close();
    }
	catch(Exception e) { 
		ExceptionHandler.invoke(e, "Please check RML mappings.");
	}

    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
  }

  
}
