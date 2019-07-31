/*
 * @(#) StreamConverter.java 	 version 1.9   12/7/2019
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.filter.text.cql2.CQL;
import org.opengis.feature.Property;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.MathTransform;
import org.openrdf.rio.RDFFormat;

import com.vividsolutions.jts.geom.Geometry;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;


/**
 * Provides a set of a streaming RDF triples in memory that can be readily serialized into a file.
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 9/3/2013
 * Modified by: Kostas Patroumpas, 27/9/2017
 * Modified: 8/11/2017, added support for system exit codes on abnormal termination
 * Modified: 19/12/2017, reorganized collection of triples using TripleGenerator
 * Modified: 24/1/2018, included auto-generation of UUIDs for the URIs of features
 * Modified: 7/2/2018, added support for exporting all available non-spatial attributes as properties
 * Modified: 14/2/2018; integrated handling of OSM records
 * Modified: 9/5/2018; integrated handling of GPX data 
 * Modified: 31/5/2018; integrated handling of classifications for OSM data
 * Modified: 18/4/2019; included support for spatial filtering over input datasets
 * Modified: 30/5/2019; correct handling of NULL geometries in CSV input files
 * Modified: 26/6/2019; added support for thematic filtering in geographical files
 * Last modified: 12/7/2019
 */

public class StreamConverter implements Converter {

	private static Configuration currentConfig;
	
	private List<Triple> results = new ArrayList<>();       //A collection of generated triples

	private TripleGenerator myGenerator;                    //Generator of triples 
	private Assistant myAssistant;							//Performs auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	private FeatureRegister myRegister = null;              //Used in registering features in the SLIPO Registry
	
	//Used in performance metrics
	private long t_start;
	private long dt;
	private int numRec;            //Number of entities (records) in input dataset
	private int rejectedRec;       //Number of rejected entities (records) from input dataset after filtering
	private int numTriples;
	    
	private BufferedWriter registryWriter = null;
	private OutputStream outFile = null;
	private StreamRDF stream;
	  
	/**
	 * Constructs a StreamConverter object that will conduct transformation at STREAM mode.	  	  
	 * @param config  User-specified configuration for the transformation process.
	 * @param assist  Assistant to perform auxiliary operations.
	 * @param outputFile  Output file that will collect resulting triples.
	 */
	public StreamConverter(Configuration config, Assistant assist, String outputFile) {
	  
	    super();
	    
	    currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DB, etc.) 
	    
	    myAssistant = assist;
	    myGenerator = new TripleGenerator(config, assist);     //Will be used to generate all triples per input feature (record)
	    
	    //Initialize performance metrics
	    t_start = System.currentTimeMillis();
	    dt = 0;
	    numRec = 0;
	    rejectedRec = 0;
		numTriples = 0;
		
		try {
			outFile = new FileOutputStream(outputFile);
		} 
		catch (FileNotFoundException e) {
		  ExceptionHandler.abort(e, "Output file not specified correctly.");
		} 
		
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
	    
		//CAUTION! Hard constraint: serialization into N-TRIPLES is only supported by Jena riot (stream) interface  
		stream = StreamRDFWriter.getWriterStream(outFile, Lang.NT);
		stream.start();             //Start issuing streaming triples
	}

	
	
	/**
	 * Provides triples resulted after applying transformation against a single input feature or a small batch of features.
	 * Applicable in STREAM transformation mode.
	 * @return A collection of RDF triples.
	 */
	public List<Triple> getTriples() {
	  
		return results;  
	}

	
	/**
	 * Parses each record from a FeatureIterator and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided via a FeatureIterator. This method is used for input from : Shapefiles, GeoJSON.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param iterator  FeatureIterator over spatial features collected from an ESRI shapefile of a GeoJSON file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(FeatureIterator<?> iterator, Classification classific, MathTransform reproject, int targetSRID, String outputFile)	  
	{
		SimpleFeatureImpl feature;
		Geometry geometry;
		String wkt = "";                 //Will hold geometry value
		List<String> columns = null;     //Non-spatial attribute names
		Filter filter = null;            //Thematic filter (handled using GeoTools)
		
		try 
		{
			//Examine whether a thematic filter has been specified
			if (currentConfig.filterSQLCondition != null)
			{
				filter = CQL.toFilter(currentConfig.filterSQLCondition);
			}
			
			//Iterate through all features	  
			while(iterator.hasNext()) 
			{
				++numRec;
			
		        feature = (SimpleFeatureImpl) iterator.next();
		        geometry = (Geometry) feature.getDefaultGeometry();

				//Apply spatial or thematic filtering (if specified by user)
				if ((!myAssistant.filterContains(geometry.toText())) || ((filter != null) && (!filter.evaluate(feature))))
				{
					rejectedRec++;
					continue;
				}
				
				//CRS transformation
		      	if (reproject != null)
		      		geometry = myAssistant.geomTransform(geometry, reproject);     
		        
		        //Get WKT representation of the transformed geometry
		      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetGeoOntology.trim());
		      					
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
		    
		        //Convert feature into a temporary tuple for conversion of all non-spatial attributes
		        Map<String,String> row = new HashMap<String, String>(columns.size());
		        for (String col : columns) {
		        	if (feature.getAttribute(col) != null)         //Exclude NULL values
		        		row.put(col, feature.getAttribute(col).toString());
		        }
							
		      	//Pass this tuple for conversion to RDF triples 
		      	String uri = myGenerator.transform(row, wkt, targetSRID, classific);
				
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(uri, row, wkt, targetSRID);
			  
			    //Periodically, collect RDF triples resulting from this batch and dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					collectTriples();
					myAssistant.notifyProgress(numRec);
				}
			}
		}
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
		finally {
			collectTriples();     //Dump any pending results into output file 
			iterator.close();
		}

	    //Store results to file
		store(outputFile);

	}
	

	/**
	 * Parses each record from a ResultSet and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided via a ResultSet. This method is used for input from a DMBS.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  ResultSet containing spatial features retrieved from a DBMS.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */			
	public void parse(ResultSet rs, Classification classific, MathTransform reproject, int targetSRID, String outputFile)
	{   
		  try
		  {
			  //Identify the names of all columns, excluding those containing spatial information
			  List<String> columns = new ArrayList<String>(rs.getMetaData().getColumnCount());
			  for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
				  if (!(rs.getMetaData().getColumnName(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (!rs.getMetaData().getColumnName(i).equalsIgnoreCase("WktGeometry")))
					  columns.add(rs.getMetaData().getColumnName(i));
			  
			  //Iterate through all records
			  while (rs.next()) 
			  {
		        //Convert resultset into a temporary tuple for conversion of all non-spatial attributes
		        Map<String,String> row = new HashMap<String, String>();
		        for (String col : columns) {
		        	row.put(col, rs.getString(col));
		        }
				
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
		      	String uri = myGenerator.transform(row, wkt, targetSRID, classific);
		      		
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(uri, row, wkt, targetSRID);
				
				++numRec;
				  
			    //Periodically, collect RDF triples resulting from this batch and dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					collectTriples();
					myAssistant.notifyProgress(numRec);
				}
			  }
		  }
		  catch(Exception e) { 
				ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		  }
		  finally {
			  collectTriples();     //Dump any pending results into output file
		  }
		  
		//Store results to file
		store(outputFile);
	}
		

	/**
	 * Parses each record from a collection of CSV records and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided by iterating over a collection of CSV records. This method is used for input from CSV.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param records  Iterator over CSV records collected from a CSV file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Iterator<CSVRecord> records, Classification classific, MathTransform reproject, int targetSRID, String outputFile)
	{
		try {	        
			//Iterate through all records
			for (Iterator<CSVRecord> iterator = records; iterator.hasNext();) {
				
	            Map<String, String> rs = iterator.next().toMap();    //Convert CSV record to map for better manipulation
	            ++numRec;

	            //Skip transformation of any features filtered out by the logical expression over thematic attributes
	            if (myAssistant.filterThematic(rs))
	            {
			    	rejectedRec++;         
					continue;
			    }
	            
		      	//Handle geometry attribute, if specified
				String wkt = null;
				if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null))       
				{    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
				    String x = rs.get(currentConfig.attrX);    //X-ordinate or longitude
				    String y = rs.get(currentConfig.attrY);    //Y-ordinate or latitude
				    if ((x != null) && (y != null) && (!x.isEmpty()) && (!y.isEmpty()))
				    	wkt = "POINT (" + x + " " + y + ")";
				}
				else if (currentConfig.attrGeometry != null)
					wkt = rs.get(currentConfig.attrGeometry);  //ASSUMPTION: Geometry values are given as WKT
				
		      	if (wkt != null)
		      	{		
					//Apply spatial filtering (if specified by user)
					if (!myAssistant.filterContains(wkt))
					{
						rejectedRec++;
						continue;
					}
					
					//CRS transformation
			      	if (reproject != null)
			      		wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//				    else	
//				        myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
		      	}
			    else 
			    {
			    	rejectedRec++;          //Skip transformation of any features with NULL geometries
					continue;
			    }
		      	
		      	//Pass this tuple for conversion to RDF triples 
		      	String uri = myGenerator.transform(rs, wkt, targetSRID, classific);
			
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(uri, rs, wkt, targetSRID);
							  
			    //Periodically, collect RDF triples resulting from this batch and dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					collectTriples();
					myAssistant.notifyProgress(numRec);
				}
			}
	    }
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
		finally {
			collectTriples();     //Dump any pending results into output file
		}

		//Store results to file	
		store(outputFile);
	}

	
	/**
	 * Parses a single OSM record and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided as an individual record. This method is used for input from OpenStreetMap XML/PBF files.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  Representation of an OSM record with attributes extracted from an OSM element (node, way, or relation).
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 */	
	public void parse(OSMRecord rs, Classification classific, MathTransform reproject, int targetSRID) 
	{	
		try {
			++numRec;
            
  	        //Parse geometric representation
			String wkt = null;
			if ((rs.getGeometry() != null) && (!rs.getGeometry().isEmpty()))
			{
				wkt = rs.getGeometry().toText();       //Get WKT representation	
				if (wkt != null)
				{				
					//Apply spatial filtering (if specified by user)
					if (!myAssistant.filterContains(wkt))
					{
						rejectedRec++;
						return;
					}
				
					//CRS transformation
			      	if (reproject != null)
			      		wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//				    else	
//				      	myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
				}
			}

			//Tags to be processed as attribute values
			Map <String, String> attrValues = new HashMap<String, String>(rs.getTagKeyValue());
           
	      	//Include standard attributes for OSM identifier, name, and type
	      	attrValues.put("osm_id", rs.getID());
	      	attrValues.put("name", rs.getName());
	      	attrValues.put("type", rs.getType());
	
            //Skip transformation of any features filtered out by the logical expression over thematic attributes
            if (myAssistant.filterThematic(attrValues))
            {
		    	rejectedRec++;         
				return;
		    }
           
		  	//Include identified category in these tags as an extra attribute
	      	if (rs.getCategory() != null)
	      	{
	      		if (currentConfig.attrCategory != null)                //Attribute to be used in the Registry as well
	      			attrValues.put(currentConfig.attrCategory, rs.getCategory());      
	      		else
	      			attrValues.put("OSM_Category", rs.getCategory());  //Ad-hoc name for this extra attribute
	      	}
	      	else
	      		return;          //CAUTION! Do not proceed to transform unless this feature complies with the filtering tags specified by the user
	      	
	      	//Process all available non-spatial attributes as specified in the collected (tag,value) pairs	
	        //... including a classification hierarchy from the OSM tags used in filtering
			String uri = myGenerator.transform(attrValues, wkt, targetSRID, classific);

			//Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, attrValues, wkt, targetSRID);
				  
		    //Periodically, collect RDF triples resulting from this batch and dump results into output file
			if (numRec % currentConfig.batch_size == 0) 
			{
				collectTriples();
				myAssistant.notifyProgress(numRec);
			}
				
		} catch (Exception e) {
			System.out.println("Problem at element with OSM id: " + rs.getID() + ". Excluded from transformation.");
		}	
		finally {
			collectTriples();     //Dump any pending results into output file
		}
	}
	
	/**
	 * Parses a single GPX waypoint/track or a single JSON node and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided as an individual record. This method is used for input data from GPX or JSON files.  
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param wkt  Well-Known Text representation of the geometry  
	 * @param attrValues  Attribute values for each thematic (non-spatial) attribute
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param geomType  The type of the geometry (e.g., POINT, POLYGON, etc.)
	 */
	public void parse(String wkt, Map <String, String> attrValues, Classification classific, int targetSRID, String geomType) 
	{	
		try {	
			++numRec;
			
			//Apply spatial filtering (if specified by user) 
			//Also skip transformation of any features filtered out by the logical expression over thematic attributes
			if ((!myAssistant.filterContains(wkt)) || (myAssistant.filterThematic(attrValues)))
			{
				rejectedRec++;
				return;
			}
            
			String uri;
	
			//Pass this tuple for conversion to RDF triples 
			if (currentConfig.attrCategory == null)
				uri = myGenerator.transform(attrValues, wkt, targetSRID, null);         //There no category specified for this feature,...
			else
				uri = myGenerator.transform(attrValues, wkt, targetSRID, classific);	//..., otherwise utilize the user-specified classification hierarchy
			
			//Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, attrValues, wkt, targetSRID);
	
		    //Periodically, collect RDF triples resulting from this batch and dump results into output file
			if (numRec % currentConfig.batch_size == 0) 
			{
				collectTriples();
				myAssistant.notifyProgress(numRec);
			}
				
		} catch (Exception e) {
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}	
		finally {
			collectTriples();     //Dump any pending results into output file
		}
	}



	/**
	 * Parses a Map structure of (key, value) pairs and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Applicable in STREAM transformation mode.
	 * Input provided as an individual record. This method is used when running over Spark/GeoSpark.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param wkt  Well-Known Text representation of the geometry
	 * @param attrValues  Attribute values for each thematic (non-spatial) attribute
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param geomType  The type of the geometry (e.g., POINT, POLYGON, etc.)
	 * @param partition_index  The index of the partition.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(String wkt, Map<String,String> attrValues, Classification classific, int targetSRID, MathTransform reproject, String geomType, int partition_index, String outputFile)
	{
		try {
			++numRec;

            //Skip transformation of any features filtered out by the logical expression over thematic attributes
            if (myAssistant.filterThematic(attrValues))
            {
		    	rejectedRec++;         
				return;
		    }
            
			if (wkt == null) {
				if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null)) {    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
					String x = attrValues.get(currentConfig.attrX);    //X-ordinate or longitude
					String y = attrValues.get(currentConfig.attrY);    //Y-ordinate or latitude
					if ((x != null) && (y != null))
						wkt = "POINT (" + x + " " + y + ")";
				} else if (currentConfig.attrGeometry != null)
					wkt = attrValues.get(currentConfig.attrGeometry);  //ASSUMPTION: Geometry values are given as WKT
			}

			if (wkt != null)
			{
				//Apply spatial filtering (if specified by user)
				if (!myAssistant.filterContains(wkt))
				{
					rejectedRec++;
					return;
				}
				//CRS transformation
				if (reproject != null)
					wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//				else
//				    myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
			}

			String uri;
			//Pass this tuple for conversion to RDF triples
			if (currentConfig.attrCategory == null)
				uri = myGenerator.transform(attrValues, wkt, targetSRID, null);         //There no category specified for this feature,...
			else
				uri = myGenerator.transform(attrValues, wkt, targetSRID, classific);	//..., otherwise utilize the user-specified classification hierarchy


			//Get a record with basic attribute that will be used for the SLIPO Registry
			if (myRegister != null)
				myRegister.createTuple(uri, attrValues, wkt, targetSRID);

			++numRec;

			//Periodically, collect RDF triples resulting from this batch and dump results into output file
			if (numRec % currentConfig.batch_size == 0)
			{
				collectTriples();
				myAssistant.notifyProgress(numRec, partition_index);
			}

		} catch (Exception e) {
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
		finally {
			collectTriples();     //Dump any pending results into output file
		}
	}

	/**
	 * Collects RDF triples generated from a batch of features (their thematic attributes and their geometries) and streamlines them to output file.
	 */
	private void collectTriples() 
	{
		try {	        		
			//Append each triple to the output stream 
			for (int i = 0; i <= myGenerator.getTriples().size()-1; i++) {
				stream.triple(myGenerator.getTriples().get(i));
				numTriples++;
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
	 * Finalizes storage of resulting tuples into a file.	
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */	
	public void store(String outputFile) 
	{
		stream.finish();               //Finished issuing triples
		
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
	    myAssistant.reportStatistics(dt, numRec, rejectedRec, numTriples, currentConfig.serialization, myGenerator.getStatistics(), myGenerator.getMBR(), currentConfig.mode, currentConfig.targetCRS, outputFile, 0);
	}

	/**
	 * Finalizes storage of resulting tuples into a file. This method is used when running over Spark/GeoSpark.
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param outputFile  Path to the output file that collects RDF triples.
	 * @param partition_index  The index of the partition.
	 */
	public void store(String outputFile, int partition_index)
	{
		stream.finish();               //Finished issuing triples

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
		myAssistant.reportStatistics(dt, numRec, rejectedRec, numTriples, currentConfig.serialization, myGenerator.getStatistics(), myGenerator.getMBR(), currentConfig.mode, currentConfig.targetCRS, outputFile, partition_index);
	}
	
	/**
	 * Retains the header (column names) of an input CSV file.
	 * Not applicable in STREAM transformation mode.		
	 */
	public void setHeader(String[] header) {

	}
		

	/**
	 * Provides access to the disk-based model consisting of transformed triples. 
     * Not applicable in STREAM transformation mode.	 
	 */
	public Model getModel() {
			  
		return null;  
	}

    /**
	 * Returns the local directory that holds the disk-based RDF graph for this transformation thread. 
	 * Not applicable in STREAM transformation mode.
	 */
    public String getTDBDir() {
    	return null;
    }

    
	/**
	 * Serializes the given RML dataset as triples written into a file. 
	 * Not applicable in STREAM transformation mode.	
	 */
	public int writeTriples(RMLDataset dataset, BufferedWriter writer, RDFFormat rdfFormat, String encoding) throws IOException {
				
		return 0;	
	}
			
	/**
	 * Serializes the given RML dataset as triples written into a file. 
	 * Not applicable in STREAM transformation mode.	
	 */			
	public int writeTriples(RMLDataset dataset, OutputStream writer, org.openrdf.rio.RDFFormat rdfFormat) throws IOException {
				
		return 0;	
	}
	
	/**
	 * Converts a row into triples according to the specified RML mappings.	 
	 * Not applicable in STREAM transformation mode.	 
	 */
	public void parseWithRML(HashMap<String, String> row, RMLDataset dataset) {
		
	}

	/**
	 * Provides the URI template used for all subjects in RDF triples concerning the classification hierarchy.
	 * Not applicable in STREAM transformation mode.		  
	 */
	public String getURItemplate4Classification() {
			  
		return null;   
	}
	  
}
