/*
 * @(#) GraphConverter.java 	 version 1.5   27/7/2018
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb.TDBFactory;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.Property;
import org.opengis.referencing.operation.MathTransform;
import org.openrdf.rio.RDFFormat;

import com.vividsolutions.jts.geom.Geometry;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;


/**
 * Creates and populates a Jena model stored on disk so that data can be serialized into a file.
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 16/2/2013
 * Modified by: Kostas Patroumpas, 27/9/2017
 * Modified: 8/11/2017, added support for system exit codes on abnormal termination
 * Modified: 19/12/2017, reorganized collection of triples using TripleGenerator
 * Modified: 24/1/2018, included auto-generation of UUIDs for the URIs of features
 * Modified: 7/2/2018, added support for exporting all available non-spatial attributes as properties
 * Modified: 14/2/2018; integrated handling of OSM records
 * Modified: 9/5/2018; integrated handling of GPX data 
 * Modified: 31/5/2018; integrated handling of classifications for OSM data
 * Last modified: 27/7/2018
 */
public class GraphConverter implements Converter {

	private static Configuration currentConfig;
	
	/**
	 * The disk-based model that will contain all RDF triples generated during transformation.
	 */
	public Model model;

	String pathTDB;                                         //Path to the local directory to hold the disk-based RDF graph for this transformation process 
	
	private BufferedWriter registryWriter = null;           //Used for the SLIPO Registry
	private TripleGenerator myGenerator;                    //Generator of triples 
	private FeatureRegister myRegister = null;              //Used in registering features in the SLIPO Registry
	
	//Used in performance metrics
	private int numRec;
	private long t_start;
	private long dt;

	/**
	 * Constructs a GraphConverter object that will conduct transformation at GRAPH mode.	  
	 * @param config  User-specified configuration for the transformation process.
	 * @param outputFile  Output file that will collect resulting triples.
	 */
	public GraphConverter(Configuration config, String outputFile) {
		  
	    super();
	    
	    currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DB, etc.)  
	
	    myGenerator = new TripleGenerator(config);     //Will be used to generate all triples per input feature (record)
	      
	    //Create a temporary directory to hold intermediate data for this graph
	    Assistant tmpAssistant = new Assistant();
	    pathTDB = tmpAssistant.createDirectory(currentConfig.tmpDir);
	    	
	    Dataset dataset = TDBFactory.createDataset(pathTDB) ;
	      
	    //The graph model to be used in the disk-based transformation process
	    this.model = dataset.getDefaultModel() ;
	      
	    //Preset some of the most common prefixes
	    this.model.setNsPrefix("geo", Constants.NS_GEO);
	    this.model.setNsPrefix("xsd", Constants.NS_XSD);
	    this.model.setNsPrefix("sf", Constants.NS_SF);
	    this.model.setNsPrefix("rdfs", Constants.NS_RDFS);
	      
	    //... as well as those specified in the configuration
	    for (int i=0; i<currentConfig.prefixes.length; i++)
	    	this.model.setNsPrefix(currentConfig.prefixes[i].trim(), currentConfig.namespaces[i].trim());
	            
		//******************************************************************
		//Specify the CSV file that will collect tuples for the SLIPO Registry
		try
		{
			if ((currentConfig.registerFeatures != false))     //Do not proceed unless the user has turned on the option for creating a .CSV file for the SLIPO Registry
			{
				myRegister = new FeatureRegister(config);      //Will be used to collect specific attribute values per input feature (record) to be registered in the SLIPO Registry
	    		myRegister.includeAttribute(config.attrKey);   //Include basic attributes from the input dataset, as specified in the configuration
	    		myRegister.includeAttribute(config.attrName);
	    		myRegister.includeAttribute(config.attrCategory);
				registryWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FilenameUtils.removeExtension(outputFile) + ".csv"), StandardCharsets.UTF_8));
				registryWriter.write(Constants.REGISTRY_CSV_HEADER);
				registryWriter.newLine();
			}
		}
		catch (Exception e) {
			 ExceptionHandler.abort(e, "Registry file not specified correctly.");
		}
		//******************************************************************
		   
		//Initialize performance metrics
		t_start = System.currentTimeMillis();
	    dt = 0;
	    numRec = 0;
	}

	
	/**
	 * Parses each record from a FeatureIterator and creates the resulting triples on a disk-based model (including geometric and non-spatial attributes).
	 * Applicable in GRAPH transformation mode.
	 * Input provided via a FeatureIterator. This method is used for input from: Shapefiles, GeoJSON.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param iterator  FeatureIterator over spatial features collected from an ESRI shapefile of a GeoJSON file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, FeatureIterator<?> iterator, Classification classific, MathTransform reproject, int targetSRID, String outputFile)	  
	{
	    SimpleFeatureImpl feature;
	    Geometry geometry;
		String wkt = "";                 //Will hold geometry value
		List<String> columns = null;     //Non-spatial attribute names
	    			    
	    try 
	    {
	      while(iterator.hasNext()) {
				
	        feature = (SimpleFeatureImpl) iterator.next();
	        geometry = (Geometry) feature.getDefaultGeometry();

		    //Determine attribute names for each feature
	        //CAUTION! This is only called for the first feature, as the structure of the rest is considered identical
		    if (columns == null)
		    {
		    	columns = new ArrayList<String>();
		    	Collection<Property> props = feature.getProperties();
		    	for (Property p: props)
		    		if ( ! p.getName().equals(feature.getDefaultGeometryProperty().getName()))       //Exclude geometry attribute
		    			columns.add(p.getName().toString());
		    }
		  
		    //Convert feature into a temporary map for conversion of all non-spatial attributes
	        Map<String,String> row = new HashMap<String, String>(columns.size());
	        for (String col : columns) {
	        	if (feature.getAttribute(col) != null)                    //Exclude NULL values
	        		row.put(col, feature.getAttribute(col).toString());
	        }
			
			//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
			String uuid = myAssistant.getUUID(currentConfig.featureSource, row.get(currentConfig.attrKey)).toString();
			
			//CRS transformation
	      	if (reproject != null)
	      		geometry = myAssistant.geomTransform(geometry, reproject);     
	        
	        //Get WKT representation of the transformed geometry
	      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetGeoOntology.trim());
	        
	      	//Pass this tuple for conversion to RDF triples  
	      	String uri = myGenerator.transform(uuid, row, wkt, targetSRID, classific);

	        //Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, row, wkt, targetSRID);
			
	      	//Collect RDF triples resulting from this tuple into the graph
	      	collectTriples();
	      	
	        myAssistant.notifyProgress(++numRec);
	      }
	    }
	    catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
	    finally {
	      iterator.close();
	    }
	    
	    //Finally, store results collected in the disk-based RDF graph
	    this.store(myAssistant, outputFile);
	}
			

	/**
	 * Parses each record from a ResultSet and creates the resulting triples on a disk-based model (including geometric and non-spatial attributes).
	 * Applicable in GRAPH transformation mode.
	 * Input provided via a ResultSet. This method is used for input from a DMBS.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  ResultSet containing spatial features retrieved from a DBMS.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */	 
	public void parse(Assistant myAssistant, ResultSet rs, Classification classific, MathTransform reproject, int targetSRID, String outputFile)
	{ 
		  try 
		  {
			  //Identify the names of all columns
			  List<String> columns = new ArrayList<String>(rs.getMetaData().getColumnCount());
			  for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
				  if (!(rs.getMetaData().getColumnName(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (!rs.getMetaData().getColumnName(i).equalsIgnoreCase("WktGeometry")))
					  columns.add(rs.getMetaData().getColumnName(i));
			  
			  //Iterate through all records
		      while (rs.next()) 
		      {					  
				  //Convert resultset into a temporary map for conversion of all non-spatial attributes
				  Map<String,String> row = new HashMap<String, String>(columns.size());
				  for (String col : columns) {  
					  row.put(col, rs.getString(col));
				  }
				  
				  //CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
				  String uuid = myAssistant.getUUID(currentConfig.featureSource, row.get(currentConfig.attrKey)).toString();
					
		          String wkt = null;
		          //Handle geometry attribute, if specified
		          if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null))       
		          {    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
		        	  String x = rs.getString(currentConfig.attrX);    //X-ordinate or longitude
		        	  String y = rs.getString(currentConfig.attrY);    //Y-ordinate or latitude
		        	  if ((x != null) && (y != null))
		        		  wkt = "POINT (" + x + " " + y + ")";
		          }
		          else if (currentConfig.attrGeometry != null)
		          {
			          if (myAssistant.pgdbDecoder != null)                //Geometry blob is read from a personal geodatabase, so its WKT must be created    	      	
			  			wkt = myAssistant.blob2WKT(rs.getBlob(currentConfig.attrGeometry), reproject);    //Also reprojected, if necessary
			          else
			          {
			          	wkt = rs.getString("WktGeometry");
	//			        myAssistant.WKT2Geometry(wkt);                  //This is done only for updating the MBR of all geometries
			          }
		          }
		          
		          //Pass this tuple for conversion to RDF triples 
		          String uri = myGenerator.transform(uuid, row, wkt, targetSRID, classific);
		        
		          //Get a record with basic attribute that will be used for the SLIPO Registry
		          if (myRegister != null)
		        	  myRegister.createTuple(uri, row, wkt, targetSRID);
		          
		          //Collect RDF triples resulting from this tuple into the graph	
		          collectTriples();
	
		          myAssistant.notifyProgress(++numRec);			        
		      }
		  }
		  catch(Exception e) { 
				ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		  }
	
		  //Finally, store results collected in the disk-based RDF graph
		  this.store(myAssistant, outputFile);  
	}
		  

	
	/**
	 * Parses each record from a collection of CSV records and creates the resulting triples on a disk-based model (including geometric and non-spatial attributes).
	 * Applicable in GRAPH transformation mode.
	 * Input provided by iterating over a collection of CSV records. This method is used for input from CSV.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param records  Iterator over CSV records collected from a CSV file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, Iterator<CSVRecord> records, Classification classific, MathTransform reproject, int targetSRID, String outputFile)
	{			    
		try {   
			//Iterate through all records
			for (Iterator<CSVRecord> iterator = records; iterator.hasNext();) {
				
	            CSVRecord rs = (CSVRecord) iterator.next();
	          
				//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
				String uuid = myAssistant.getUUID(currentConfig.featureSource, (rs.isSet(currentConfig.attrKey) ? rs.get(currentConfig.attrKey) : null)).toString();
				
		      	//Handle geometry attribute, if specified
				String wkt = null;
				if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null))       
				{    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
				    String x = rs.get(currentConfig.attrX);    //X-ordinate or longitude
				    String y = rs.get(currentConfig.attrY);    //Y-ordinate or latitude
				    if ((x != null) && (y != null))
				    	wkt = "POINT (" + x + " " + y + ")";
				}
				else if (currentConfig.attrGeometry != null)
					wkt = rs.get(currentConfig.attrGeometry);  //ASSUMPTION: Geometry values are given as WKT
				
		      	if (wkt != null)
		      	{							
					//CRS transformation
			      	if (reproject != null)
			      		wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//					else	
//					    myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
		      	}

		      	//Pass this tuple for conversion to RDF triples 
		      	String uri = myGenerator.transform(uuid, rs.toMap(), wkt, targetSRID, classific);
		      
		        //Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(uri, rs.toMap(), wkt, targetSRID);
				
		      	//Collect RDF triples resulting from this tuple into the graph
		      	collectTriples();
		      	
				myAssistant.notifyProgress(++numRec);		
			}
	    }
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}

		//Finally, store results collected in the disk-based RDF graph
		this.store(myAssistant, outputFile);
	}
			
	/**
	 * Parses a single OSM record and creates the resulting triples on a disk-based model (including geometric and non-spatial attributes).
	 * Applicable in GRAPH transformation mode.
	 * Input provided as an individual record. This method is used for input from OpenStreetMap XML/PBF files.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  Representation of an OSM record with attributes extracted from an OSM element (node, way, or relation).
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 */		
	public void parse(Assistant myAssistant, OSMRecord rs, Classification classific, MathTransform reproject, int targetSRID) 
	{	
		try {		
			//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
			String uuid = myAssistant.getUUID(currentConfig.featureSource + rs.getID()).toString();
			
  	        //Parse geometric representation
			String wkt = null;
			if ((rs.getGeometry() != null) && (!rs.getGeometry().isEmpty()))
			{
				wkt = rs.getGeometry().toText();       //Get WKT representation	
				if (wkt != null)
				{							
					//CRS transformation
			      	if (reproject != null)
			      		wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//					else	
//					    myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
				}
			}
			
			//Tags to be processed as attribute values
			Map <String, String> attrValues = new HashMap<String, String>(rs.getTagKeyValue());
			
	      	//Include attributes for OSM identifier, name, and type
	      	attrValues.put("osm_id", rs.getID());
	      	attrValues.put("name", rs.getName());
	      	attrValues.put("type", rs.getType());
	      	
		  	//Include identified category in these tags as an extra attribute
	      	if (rs.getCategory() != null)
	      	{
	      		if (currentConfig.attrCategory != null)                //Attribute to be used in the Registry as well
	      			attrValues.put(currentConfig.attrCategory, rs.getCategory());      
	      		else
	      			attrValues.put("OSM_Category", rs.getCategory());  //Ad-hoc name for this extra attribute
	      	}
	      	else
	      		return;          //CAUTION! Do not proceed to transform unless this feature does NOT comply with the filtering tags specified by the user
	      	
	      	//Process all available non-spatial attributes as specified in the collected (tag,value) pairs	
	        //... including a classification hierarchy from the OSM tags used in filtering
			String uri = myGenerator.transform(uuid, attrValues, wkt, targetSRID, classific);

			//Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, attrValues, wkt, targetSRID);
			
	      	//Collect RDF triples resulting from this tuple into the graph
	      	collectTriples();
	      	
			myAssistant.notifyProgress(++numRec);
				
		} catch (Exception e) {
			System.out.println("Problem at element with OSM id: " + rs.getID() + ". Excluded from transformation.");
			System.out.println("RECORD: " + rs.toString());
		}	
	}
			

	/**
	 * Parses a single GPX waypoint/track or a single JSON node and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in GRAPH transformation mode.
	 * Input provided as an individual record. This method is used for input data from GPX or JSON files.  
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param uuid  The UUID to be assigned to the URIs in the resulting triples
	 * @param wkt  Well-Known Text representation of the geometry  
	 * @param attrValues  Attribute values for each thematic (non-spatial) attribute
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param geomType  The type of the geometry (e.g., POINT, POLYGON, etc.)
	 */
	public void parse(Assistant myAssistant, String uuid, String wkt, Map <String, String> attrValues, int targetSRID, String geomType) 
	{	
		try {
	
			//Pass this tuple for conversion to RDF triples 
			//FIXME: Specify a classification hierarchy for the features
	      	String uri = myGenerator.transform(uuid, attrValues, wkt, targetSRID, null);
 
			//Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, attrValues, wkt, targetSRID);
			
			//Collect RDF triples resulting from this tuple into the graph
	      	collectTriples();
	      	
			myAssistant.notifyProgress(++numRec);
				
		} catch (Exception e) {
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}	
		
	}

	/**
	 * 	Collects RDF triples generated from a given feature (its thematic attributes and its geometry) and stores them into the disk-based RDF graph.
	 */
	private void collectTriples() 
	{
		try {	        		
			//Add resulting triples to the RDF graph
	        for (Triple t : myGenerator.getTriples()) {
	    		Statement stmt = model.asStatement(t);
	    		model.add(stmt);
	    	}
			
	        //Clean up RDF triples, in order to collect the new ones derived from the next batch of features
			myGenerator.clearTriples();
			
			//******************************************************************
			//Keep all attribute values required for registering in the SLIPO Registry
			if ((registryWriter != null) && (!myRegister.getTuples4Registry().isEmpty()))
			{
				for (String aTuple: myRegister.getTuples4Registry())
				{
					registryWriter.write(aTuple);
					registryWriter.newLine();
				}
			
				//Clean up tuples for SLIPO Registry, in order to collect the new ones derived from the next batch of features
				myRegister.clearTuples4Registry();
			}
			//******************************************************************
			
		}
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
		
		
	}
			

	/**
	 * Stores resulting tuples into a file.	
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */		
	public void store(Assistant myAssistant, String outputFile)
	{
	    dt = System.currentTimeMillis() - t_start;
	    System.out.println(myAssistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
	    System.out.println(myAssistant.getGMTime() + " Started writing triples to file...");
	    
	    //Fetch resulting triples as stored in the model
	    //model = this.getModel();
	    
	    //Count the number of statements in the graph
	    int numStmt = this.getModel().getGraph().size();
	    try {
		    //Export model to a suitable serialization format
		    FileOutputStream out = new FileOutputStream(outputFile);
		    this.getModel().write(out, currentConfig.serialization);
	    }
	    catch(Exception e) { 
			ExceptionHandler.abort(e, "Serialized output cannot be written into a file. Please check configuration file.");
		}
//	    finally {
//	    	//Remove all files created in the temporary directory
//	    	myAssistant.removeDirectory(pathTDB);
//	    }
	    
		//******************************************************************
		//Close the file that will collect all tuples for the SLIPO Registry
		try {
			if (registryWriter != null)
				registryWriter.close();
		} catch (IOException e) {
			ExceptionHandler.abort(e, "An error occurred during creation of the file for Registry.");
		}
		//******************************************************************
		
		//Measure execution time and issue statistics on the entire process
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, myGenerator.getStatistics(), currentConfig.mode, outputFile);
	    myGenerator.getStatistics();
	}
			

	/**
	 * Retains the header (column names) of an input CSV file.
	 * Not applicable in GRAPH transformation mode.		
	 */
	public void setHeader(String[] header) {		
	
	}
			

	/**
     * Provides access to the disk-based model consisting of transformed triples. Applicable in GRAPH transformation mode.
     * @return The model created for a dataset (RDF graph) stored on disk.
     */	  
	public Model getModel() {
  
		return this.model;
	}

	/**
	 * Returns the local directory that holds the disk-based RDF graph for this transformation thread.
	 * @return  Path to the local directory. 
	 */
	public String getTDBDir() {
		return pathTDB;
	}
	
	/**
	 * Provides triples resulted from transformation.
	 * Not applicable in GRAPH transformation mode.		  
	 */
	public List<Triple> getTriples() {
  
		return null;  
	}

	
	/**
	 * Serializes the given RML dataset as triples written into a file. 
	 * Not applicable in GRAPH transformation mode.	
	 */
	public int writeTriples(RMLDataset dataset, BufferedWriter writer, RDFFormat rdfFormat, String encoding) throws IOException {
				
		return 0;	
	}
			
	/**
	 * Serializes the given RML dataset as triples written into a file. 
	 * Not applicable in GRAPH transformation mode.	
	 */			
	public int writeTriples(RMLDataset dataset, OutputStream writer, org.openrdf.rio.RDFFormat rdfFormat) throws IOException {
				
		return 0;	
	}
			
		  
	/**
	 * Converts a row into triples according to the specified RML mappings.	 
	 * Not applicable in GRAPH transformation mode.	 
	 */
	public void parseWithRML(HashMap<String, String> row, RMLDataset dataset) {
		
	}

	/**
	 * Provides the URI template used for all subjects in RDF triples concerning the classification hierarchy.
	 * Not applicable in GRAPH transformation mode.		  
	 */
	public String getURItemplate4Classification() {
			  
		return null;   
	}

	
}
