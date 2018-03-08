/*
 * @(#) RMLDatasetConverter.java 	 version 1.4   8/3/2018
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
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.operation.MathTransform;
import org.openrdf.repository.Repository;

import com.vividsolutions.jts.geom.Geometry;

import be.ugent.mmlab.rml.mapdochandler.extraction.std.StdRMLMappingFactory;
import be.ugent.mmlab.rml.mapdochandler.retrieval.RMLDocRetrieval;
import be.ugent.mmlab.rml.model.RMLMapping;
import be.ugent.mmlab.rml.model.TriplesMap;
import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.model.dataset.SimpleRMLDataset;
import be.ugent.mmlab.rml.performer.NodeRMLPerformer;
import be.ugent.mmlab.rml.performer.RMLPerformer;
import be.ugent.mmlab.rml.processor.RMLProcessor;
import be.ugent.mmlab.rml.processor.RMLProcessorFactory;
import be.ugent.mmlab.rml.processor.concrete.ConcreteRMLProcessorFactory;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * Creates and populates a RML dataset so that data can be serialized into a file.
 * @author Kostas Patroumpas
 * @version 1.4
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/9/2013
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 18/2/2018; Included attribute statistics calculated during transformation
 * TODO: This mode does NOT currently include support for the SLIPO Registry.
 * Last modified by: Kostas Patroumpas, 8/3/2018
 */
public class RMLConverter implements Converter {

	private static Configuration currentConfig; 
	
//	RMLDataset dataset;
	RMLPerformer[] performers;
	String[] exeTriplesMap = null;
	Map<String, String> parameters = null;
	TriplesMap[] maps;
	String templateCategoryURI;

	Map<String, Integer> attrStatistics;   //Statistics for each attribute
	
	String[] csvHeader;                    //Used when handling CSV records

	//Used in performance metrics
	private long t_start;
	private long dt;
	private int numRec;
	private int numTriples;
	
	/**
	 * Constructs a RMLConverter object that will conduct transformation at RML mode.
	 * @param config  User-specified configuration for the transformation process.
	 */
	public RMLConverter(Configuration config) {
		  
	      super();
	    
	      currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DBMS, etc.)  

	      attrStatistics = new HashMap<String, Integer>();
	      
	      try {
				StdRMLMappingFactory mappingFactory = new StdRMLMappingFactory();

				RMLDocRetrieval mapDocRetrieval = new RMLDocRetrieval();
				Repository repository = mapDocRetrieval.getMappingDoc(currentConfig.mappingSpec, org.openrdf.rio.RDFFormat.TURTLE);
				RMLMapping mapping = mappingFactory.extractRMLMapping(repository);
							
				Collection<TriplesMap> triplesMaps;
				triplesMaps = mapping.getTriplesMaps();
				
				maps = triplesMaps.toArray(new TriplesMap[triplesMaps.size()]);
				performers = new RMLPerformer[maps.length];
				
				RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();
				int k = 0;
				for (TriplesMap map : triplesMaps) 
				{	
					//System.out.println(map.getSubjectMap().getStringTemplate());
					
					//Original call to RML processor
					//RMLProcessor subprocessor = factory.create(map.getLogicalSource().getReferenceFormulation(), parameters, map);
					
					//Simplified RML processor without reference to logical source (assuming a custom row as input)
					RMLProcessor subprocessor = factory.create(map);
					
					//Identify the template used for URIs regarding classification
					//FIXME: Assuming that a single attribute "CATEGORY_ID" is always used for constructing the URIs for categories
					if ((map.getSubjectMap().getStringTemplate() != null) && (map.getSubjectMap().getStringTemplate().contains("CATEGORY_ID")))
						templateCategoryURI = map.getSubjectMap().getStringTemplate();
						
					performers[k] = new NodeRMLPerformer(subprocessor);
					k++;
				}
	      } catch(Exception e) { 
	  	    	ExceptionHandler.abort(e, " An error occurred while creating RML processors with the given triple mappings."); 
	      }  
	      
	      //Initialize performance metrics
	      t_start = System.currentTimeMillis();
	      dt = 0;
	      numRec = 0;
	      numTriples = 0;  
	}

	  
	/**
	 * Update statistics for the given attribute during transformation.
	 * @param attrKey  Attribute name for which to update statistics.
	 */
	private void updateStatistics(String attrKey) {

		if ((attrStatistics.get(attrKey)) == null)
			attrStatistics.put(attrKey, 1);                                  //First occurrence of this attribute
		else
			attrStatistics.replace(attrKey, attrStatistics.get(attrKey)+1);  //Update count of NOT NULL values for this attribute
	  }
	  
	
	/**
	 * Provides statistics collected for each attribute after transformation.  
	 * @return  Count of transformed values per attribute.
	 */
	public Map<String, Integer> getStatistics() {

		  return attrStatistics;
	}
	 

	/**
	 * Parses each record from a FeatureIterator and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided via a FeatureIterator (used with input formats: Shapefiles, GeoJSON).
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
	    String wkt = "";
	    boolean schemaFixed = false;
	    List<String> attrNames = null;
/*    
		//Determine the attribute schema of these features  
		final SimpleFeatureType original = (SimpleFeatureType) featureCollection.getSchema();
		List<String> attrNames = new ArrayList<String>();
		for (AttributeDescriptor ad : original.getAttributeDescriptors()) 
		{
		    final String name = ad.getLocalName();
		    if(!"boundedBy".equals(name) && !"metadataProperty".equals(name)) 
		    	attrNames.add(name);
		}
*/    

//  	RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    	  
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
			
			//Iterate through all features
			while(iterator.hasNext()) {
	  
				feature = (SimpleFeatureImpl) iterator.next();
				
				//Determine the attribute schema according to the first one of these features 
				if (!schemaFixed)
				{
					SimpleFeatureType original = feature.getFeatureType(); //(SimpleFeatureType) featureCollection.getSchema();
					attrNames = new ArrayList<String>();
					for (AttributeDescriptor ad : original.getAttributeDescriptors()) 
					{
					    final String name = ad.getLocalName();
					    if(!"boundedBy".equals(name) && !"metadataProperty".equals(name)) 
					    	attrNames.add(name);
					}
					schemaFixed = true;
				}
				
				//Handle geometry
				geometry = (Geometry) feature.getDefaultGeometry();
				  
				//CRS transformation
		      	if (reproject != null)
		      		geometry = myAssistant.geomTransform(geometry, reproject);     
		        
		        //Get WKT representation of the transformed geometry
		      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetGeoOntology.trim());
		      	
		      	//Pass all NOT NULL attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
		      	for (int i=0; i<feature.getNumberOfAttributes(); i++)
		      	{
		      		if ((!attrNames.get(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (feature.getAttribute(i) != null) && (!feature.getAttribute(i).toString().equals("")))
		      		{
		      		    //Names of attributes in upper case; case-sensitive in RML mappings!
		      			row.put(attrNames.get(i).toUpperCase(), feature.getAttribute(i).toString());
		      			updateStatistics(attrNames.get(i).toUpperCase());          //Update count of NOT NULL values transformed for this attribute
		      		}
		      	}
		        row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   

		      	//Include a category identifier, as found in the classification scheme
		      	if (classific != null)
		      		row.put("CATEGORY_URI", classific.getUUID(feature.getAttribute(currentConfig.attrCategory).toString()));
		      	System.out.println(feature.getAttribute(currentConfig.attrCategory).toString() + " " + classific.findUUID(feature.getAttribute(currentConfig.attrCategory).toString()));
		      	
		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	row.put("UUID", myAssistant.getUUID(feature.getAttribute(currentConfig.attrKey).toString()).toString());
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
		       
		        ++numRec;
			  
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					numTriples += this.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}
		finally {
			iterator.close();
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, getStatistics(), currentConfig.mode, outputFile);		    
		  
	}


	/**
	 * Parses each record from a ResultSet and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided via a ResultSet (used with input from a DMBS).
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  ResultSet containing spatial features retrieved from a DBMS.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */	  
	public void parse(Assistant myAssistant, ResultSet rs, Classification classific, MathTransform reproject, int targetSRID, String outputFile) 
	{
//      RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    
		OutputStream outFile = null;
		try {
			outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
		} 
		catch (FileNotFoundException e) {
			ExceptionHandler.abort(e, "Output file not specified correctly.");
		}
		  
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outFile, "UTF-8"));

			//Iterate through all records
		    while (rs.next()) 
		    {
		      	//Pass all NOT NULL attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
		      	for (int i=1; i<=rs.getMetaData().getColumnCount(); i++)
		      	{
		      		if ((!rs.getMetaData().getColumnLabel(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (!rs.getMetaData().getColumnName(i).equalsIgnoreCase("WktGeometry")) && (rs.getString(i) != null))
		      		{
		      		    //Names of attributes in upper case; case-sensitive in RML mappings!
		      			row.put(rs.getMetaData().getColumnLabel(i).toUpperCase(), rs.getString(i));
		      			updateStatistics(rs.getMetaData().getColumnLabel(i).toUpperCase());          //Update count of NOT NULL values transformed for this attribute
		      		}
		      	}
		      	
		      	//Include a category identifier, as found in the classification scheme
		      	if (classific != null)
		      		row.put("CATEGORY_URI", classific.getUUID(rs.getString(currentConfig.attrCategory)));
		      	//System.out.println(classification.getURI(rs.getString(currentConfig.attrCategory)));
		      	
		      	String wkt = null;
		      	//Handle geometry attribute, if specified
				if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null))       
				{    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
				    String x = rs.getString(currentConfig.attrX);    //X-ordinate or longitude
				    String y = rs.getString(currentConfig.attrY);    //Y-ordinate or latitude
					wkt = "POINT (" + x + " " + y + ")";
				}
				else if (currentConfig.attrGeometry != null)
		      	{	        
			        if (myAssistant.pgdbDecoder != null)                //Geometry blob is read from a personal geodatabase, so its WKT must be created    	      	
			  			wkt = myAssistant.blob2WKT(rs.getBlob(currentConfig.attrGeometry), reproject);    //Also reprojected, if necessary
			        else
			        {
			          	wkt = rs.getString("WktGeometry");
//					    myAssistant.WKT2Geometry(wkt);                  //This is done only for updating the MBR of all geometries
			        }	
		      	}
		      	
				if (wkt != null)
					row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Update attribute for the geometry as WKT along with the CRS
			     
		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	row.put("UUID", myAssistant.getUUID(rs.getString(currentConfig.attrKey)).toString());
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
		       
		        ++numRec;
			  
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
	    			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
					dataset = new SimpleRMLDataset();	   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, getStatistics(), currentConfig.mode, outputFile);  
	}


	/**
	 * Parses each record from a collection of CSV records and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided by iterating over a collection of CSV records (used with input from CSV).
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param records  Iterator over CSV records collected from a CSV file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, Iterator<CSVRecord> records, Classification classific, MathTransform reproject, int targetSRID, String outputFile) 
	{
//	    RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    	  
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
			
			//Iterate through all records
			for (Iterator<CSVRecord> iterator = records; iterator.hasNext();) {
				
	            CSVRecord rs = (CSVRecord) iterator.next();
	            
		      	//Pass all attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
		      	for (int i=0; i<rs.size(); i++)
		      	{
		      		//Names of attributes are case-sensitive in RML mappings; by convention, they must be written in upper case
		      		if (rs.get(i) != null)
		      		{
		      			row.put(csvHeader[i].toUpperCase(), rs.get(i));
		      			updateStatistics(csvHeader[i].toUpperCase());          //Update count of NOT NULL values transformed for this attribute
		      		}    		
		      	}
		      	
		      	//Handle geometry attribute, if specified
				String wkt = null;
				if ((currentConfig.attrGeometry == null) && (currentConfig.attrX != null) && (currentConfig.attrY != null))       
				{    //In case no single geometry attribute with WKT values is specified, compose WKT from a pair of coordinates
				    String x = rs.get(currentConfig.attrX);    //X-ordinate or longitude
				    String y = rs.get(currentConfig.attrY);    //Y-ordinate or latitude
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
			      	
		      		row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Extra attribute for the geometry as WKT
		      	}
		      
		      	//Include a category identifier, as found in the classification scheme
		      	if (classific != null)
		      		row.put("CATEGORY_URI", classific.getUUID(rs.get(currentConfig.attrCategory)));
		      	//System.out.println(classification.getURI(rs.getString(currentConfig.attrCategory)));

		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	row.put("UUID", myAssistant.getUUID(rs.get(currentConfig.attrKey)).toString());
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
		       
		        ++numRec;
			  
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!	
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, getStatistics(), currentConfig.mode, outputFile); 
	}
	

	/**
	 * Parses a single OSM record and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping.
	 * Input provided as an individual record (used with input format: OpenStreetMap XML).
	 * TODO: Not implemented for RML transformation mode.
	 */
	public void parse(Assistant myAssistant, OSMRecord rs, MathTransform reproject, int targetSRID) {
	
	}
			

	/**
	 * Stores resulting tuples into a file.	
	 * Not applicable in RML transformation mode.
	 */	  
	public void store(Assistant myAssistant, String outputFile) {
  
	}
			

	/**
	 * Retains the header (column names) of an input CSV file.
	 * @param header  A array of attribute (column) names.
	 */		
	public void setHeader(String[] header) {

		csvHeader = header;                   //Header is contained in the first line of the input CSV file	
	}
			
	/**
	 * Provides the disk-based model consisting of transformed triples.
	 * Not applicable in RML transformation mode.
	 */
	public Model getModel() {
	  
		return null;  
	}

    /**
	 * Returns the local directory that holds the disk-based RDF graph for this transformation thread. 
	 * Not applicable in RML transformation mode.
	 */
    public String getTDBDir() {
    	return null;
    }
    
	/**
	 * Provides triples resulting from transformation.	  
	 * Not applicable in RML transformation mode.
	 */
	public List<Triple> getTriples() {

		return null; 
	}

	
	/**
	 * Converts a row (with several attribute values, including geometry) into suitable triples according to the specified RML mappings. 
	 * Applicable in RML transformation mode only.
	 * @param row  Record with attribute names and their respective values.
	 * @param dataset RMLDataset to collect the resulting triples.
	 */
	public void parseWithRML(HashMap<String, String> row, RMLDataset dataset)
	{
		  //Parse the given record with each of the performers specified in the RML mappings
	      for (int j=0; j<performers.length; j++)
	      {
	    	  performers[j].perform(row, dataset, maps[j], exeTriplesMap, parameters, false);
	      }	
	}
	

	/**
	 * Provides the URI template used for all subjects in RDF triples concerning the classification hierarchy.
	 * Applicable in RML transformation mode.
	 * @return A URI to be used as template for triples regarding classification.
	 */	
	public String getURItemplate4Classification()
	{
		return templateCategoryURI;
	}


    /**
     * Serializes the given RML dataset as triples written into a file. 
     * Applicable in RML transformation mode.
     * @param dataset RMLDataset that has collected the resulting triples.
     * @param writer  BufferedWriter to write triples in the output stream.
     * @param rdfFormat  Serialization format of triples to be written to the file.
     * @param encoding  Encoding for string literals.
     * @return  The number of triples written to the output file.
     * @throws IOException
     */	  
	public int writeTriples(RMLDataset dataset, BufferedWriter writer, org.openrdf.rio.RDFFormat rdfFormat, String encoding)
	{
	  	int numTriples = 0;
	  	try {
		    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			dataset.dumpRDF(byteStream, rdfFormat);
			writer.write(StringEscapeUtils.unescapeJava(byteStream.toString(encoding)));
			numTriples = dataset.getSize();

			dataset.closeRepository();
	  	}
  	    catch(Exception e) { 
  	    	ExceptionHandler.abort(e, " An error occurred when dumping RDF triples into the output file.");
  	    }
	  			  
		return numTriples;
	}
		  
		  
	/**
     * Serializes the given RML dataset as triples written into a file.
     * Applicable in RML transformation mode only.
     * @param dataset RMLDataset that has collected the resulting triples.
     * @param writer  BufferedWriter to write triples in the output stream.
     * @param rdfFormat  Serialization format of triples to be written to the file.
	 * @return  The number of triples written to the output file.
	 * @throws IOException
	 */
	public int writeTriples(RMLDataset dataset, OutputStream writer, org.openrdf.rio.RDFFormat rdfFormat)
	{
	  	int numTriples = 0;
	  	try {
		    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			dataset.dumpRDF(byteStream, rdfFormat);
			
			writer.write(byteStream.toByteArray()); 
			numTriples = dataset.getSize();
	
			dataset.closeRepository();
	  	}
  	    catch(Exception e) { 
  	    	ExceptionHandler.abort(e, " An error occurred when dumping RDF triples into the output file.");
  	    }
	  			  
		return numTriples;
	}

}
