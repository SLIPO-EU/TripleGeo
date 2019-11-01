/*
 * @(#) RMLDatasetConverter.java  version 2.0  10/10/2019
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
import java.io.ByteArrayOutputStream;
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

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.filter.text.cql2.CQL;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.MathTransform;
import org.openrdf.repository.Repository;

import com.vividsolutions.jts.geom.Envelope;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * Creates and populates a RML dataset so that data can be serialized into a file.
 * LIMITATIONS: - No support for dynamically executed built-in functions (e.g., for calculating on-the-fly the area or length of geometries). 
 *              - By convention, attribute names referring to input data must be specified in UPPER-CASE letters in the RML mappings (in .TTL files).
 *              - Currently, no support for transformation from OSM or GPX or on top of Spark.
 * @author Kostas Patroumpas
 * @version 2.0
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/9/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 18/2/2018; included attribute statistics calculated during transformation
 * Modified: 22/4/2019; included support for spatial filtering over input datasets
 * Modified: 9/10/2019; included support for thematic filtering; also exporting basic attributes for the SLIPO Registry.
 * Last modified by: Kostas Patroumpas, 10/10/2019
 */
public class RMLConverter implements Converter {

	private static Configuration currentConfig; 
	
	private BufferedWriter registryWriter = null;           //Used for the SLIPO Registry
	private Assistant myAssistant;							//Performs auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	private FeatureRegister myRegister = null;              //Used in registering features in the SLIPO Registry
	
//	RMLDataset dataset;
	RMLPerformer[] performers;
	String[] exeTriplesMap = null;
	Map<String, String> parameters = null;
	TriplesMap[] maps;
	String paramFeatureURI = "{UUID}";				//Assuming that a single attribute "UUID" is always used for constructing the URIs of features
	String templateFeatureURI;
	String paramCategoryURI = "{CATEGORY_URI}";   	//Assuming that a single attribute "CATEGORY_URI" is always used for constructing the URIs for categories
	String templateCategoryURI;
	String attrAssignedCategory = "ASSIGNED_CATEGORY";    //Attribute used to denote an embedded category, after mapping of user-defined category to the default classification scheme
	
	Map<String, Integer> attrStatistics;   //Statistics for each attribute
	
	String[] csvHeader;                    //Used when handling CSV records

	//Used in performance metrics
	private long t_start;
	private long dt;
	private int numRec;            //Number of entities (records) in input dataset
	private int rejectedRec;       //Number of rejected entities (records) from input dataset after filtering
	private int numTriples;
	public Envelope mbr;           //Minimum Bounding Rectangle (in WGS84) of all geometries handled during a given transformation process
	
	/**
	 * Constructs a RMLConverter object that will conduct transformation at RML mode.
	 * @param config  User-specified configuration for the transformation process.
	 * @param assist  Assistant to perform auxiliary operations.
	 * @param outputFile  Output file that will collect resulting triples.
	 */
	public RMLConverter(Configuration config, Assistant assist, String outputFile) {
		  
	      super();
	    
	      currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DBMS, etc.)  

	      myAssistant = assist;
	      
	      attrStatistics = new HashMap<String, Integer>();
	      
	      //Initialize MBR of transformed geometries
	      mbr = new Envelope();
	      mbr.init();
			
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
					
					//Identify the template used for URIs of features
					if ((map.getSubjectMap().getStringTemplate() != null) && (map.getSubjectMap().getStringTemplate().endsWith(paramFeatureURI)))
						templateFeatureURI = map.getSubjectMap().getStringTemplate();
					
					//Identify the template used for URIs regarding classification
					if ((map.getSubjectMap().getStringTemplate() != null) && (map.getSubjectMap().getStringTemplate().endsWith(paramCategoryURI)))
						templateCategoryURI = map.getSubjectMap().getStringTemplate();
						
					performers[k] = new NodeRMLPerformer(subprocessor);
					k++;
				}
	      } catch(Exception e) { 
	  	    	ExceptionHandler.abort(e, " An error occurred while creating RML processors with the given triple mappings."); 
	      }  

			//******************************************************************
			//Specify the CSV file that will collect tuples for the SLIPO Registry
			try
			{
				if ((currentConfig.registerFeatures != false))     //Do not proceed unless the user has turned on the option for creating a .CSV file for the SLIPO Registry
				{
					myRegister = new FeatureRegister(config);      //Will be used to collect specific attribute values per input feature (record) to be registered in the SLIPO Registry
					//Include basic attributes from the input dataset with their names
					//CAUTION! Attribute names are specified in upper case since they are case-sensitive in RML mappings!
		    		myRegister.includeAttribute(config.attrKey.toUpperCase());
		    		myRegister.includeAttribute(config.attrName.toUpperCase());
		    		myRegister.includeAttribute((attrAssignedCategory != null) ? attrAssignedCategory : config.attrCategory.toUpperCase());
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
	      numTriples = 0;  
	      rejectedRec = 0;
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
	 * Updates the MBR of the geographic dataset as this is being transformed. 	
	 * @param g  Geometry that will be checked for possible expansion of the MBR of all features processed thus far
	 */
	public void updateMBR(Geometry g) {
		
		Envelope env = g.getEnvelopeInternal();          //MBR of the given geometry
		if ((mbr== null) || (mbr.isNull()))
			mbr = env;
		else if (!mbr.contains(env))
			mbr.expandToInclude(env);                    //Expand MBR is the given geometry does not fit in
	}
	
	/**
	 * Provides the MBR of the geographic dataset that has been transformed.
	 * @return  The MBR of the transformed geometries.
	 */
	public Envelope getMBR() {
		if ((mbr == null) || (mbr.isNull()))
			return null;
		return mbr;
	}
	
	/**
	 * Parses each record from a FeatureIterator and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided via a FeatureIterator (used with input formats: Shapefiles, GeoJSON).
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
	    String wkt = "";
	    boolean schemaFixed = false;
	    List<String> attrNames = null;
	    Filter filter = null;            //Thematic filter (handled using GeoTools for ESRI shapefiles and GeoJSON formats)    

//  	RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    	  
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));

			//Examine whether a thematic filter has been specified
			if (currentConfig.filterSQLCondition != null)
			{
				filter = CQL.toFilter(currentConfig.filterSQLCondition);
			}
			
			//Iterate through all features
			while(iterator.hasNext()) {
	  
				feature = (SimpleFeatureImpl) iterator.next();
				++numRec;
				
				//Determine the attribute schema according to the first one of these features 
				if (!schemaFixed)
				{
					SimpleFeatureType original = feature.getFeatureType(); //(SimpleFeatureType) featureCollection.getSchema();
					attrNames = new ArrayList<String>();
					for (AttributeDescriptor ad : original.getAttributeDescriptors()) 
					{
					    final String name = ad.getLocalName();
					    if (!"boundedBy".equals(name) && !"metadataProperty".equals(name)) 
					    	attrNames.add(name);
					}
					schemaFixed = true;
				}
				
				//Handle geometry
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

		  	  	//Insert extra attributes concerning lon/lat coordinates for the centroid 
		  	  	Geometry geomProjected = myAssistant.geomTransformWGS84(wkt, targetSRID);
		  	  	updateMBR(geomProjected);                 //Keep the MBR of transformed geometries up-to-date
		  	  	
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
		      	{
		      		row.put("CATEGORY_URI", classific.getUUID(feature.getAttribute(currentConfig.attrCategory).toString()));
		      		//Also determine the name of the embedded category assigned in the default classification scheme
					if (attrAssignedCategory != null)
						row.put(attrAssignedCategory, classific.getEmbeddedCategory(feature.getAttribute(currentConfig.attrCategory).toString()));
		      	}
		      	
		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	String uri = myAssistant.getUUID(feature.getAttribute(currentConfig.attrKey).toString()).toString();
		      	row.put("UUID", uri);
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
			  
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(templateFeatureURI.replace("{UUID}", uri), row, wkt, targetSRID);
				
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					numTriples += this.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!
					collectTuples4Registry();              //Basic attributes written to a .CSV for the SLIPO Registry
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
			collectTuples4Registry();              //Also, basic attributes are written to a .CSV for the SLIPO Registry
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}
		finally {
			iterator.close();
			store(outputFile);     //Dump any pending results into output file (under RML mode, this is used for the SLIPO Registry only)
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, rejectedRec, numTriples, currentConfig.serialization, getStatistics(), getMBR(), currentConfig.mode, currentConfig.targetCRS, outputFile, 0);		    
		  
	}


	/**
	 * Parses each record from a ResultSet and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided via a ResultSet (used with input from a DMBS).
	 * @param rs  ResultSet containing spatial features retrieved from a DBMS.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */	  
	public void parse(ResultSet rs, Classification classific, MathTransform reproject, int targetSRID, String outputFile) 
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
		      	{
		      		row.put("CATEGORY_URI", classific.getUUID(rs.getString(currentConfig.attrCategory)));
		      		//Also determine the name of the embedded category assigned in the default classification scheme
					if (attrAssignedCategory != null)
						row.put(attrAssignedCategory, classific.getEmbeddedCategory(rs.getString(currentConfig.attrCategory)));
		      	}	      	
		      	
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
				{
			  	  	//Insert extra attributes concerning lon/lat coordinates for the centroid 
			  	  	Geometry geomProjected = myAssistant.geomTransformWGS84(wkt, targetSRID);
			  	  	updateMBR(geomProjected);                 //Keep the MBR of transformed geometries up-to-date
					row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Update attribute for the geometry as WKT along with the CRS
				}
				
		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	String uri = myAssistant.getUUID(rs.getString(currentConfig.attrKey)).toString();
		      	row.put("UUID", uri);
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
			  
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(templateFeatureURI.replace("{UUID}", uri), row, wkt, targetSRID);
				
		        ++numRec;
			  
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
	    			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
					dataset = new SimpleRMLDataset();	   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
					collectTuples4Registry();              //Basic attributes written to a .CSV for the SLIPO Registry
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
			collectTuples4Registry();              //Also, basic attributes are written to a .CSV for the SLIPO Registry
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}
		finally {
			store(outputFile);     //Dump any pending results into output file (under RML mode, this is used for the SLIPO Registry only)
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, rejectedRec, numTriples, currentConfig.serialization, getStatistics(), getMBR(), currentConfig.mode, currentConfig.targetCRS, outputFile, 0);  
	}


	/**
	 * Parses each record from a collection of CSV records and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping. 
	 * Applicable in RML transformation mode.
	 * Input provided by iterating over a collection of CSV records (used with input from CSV).
	 * @param records  Iterator over CSV records collected from a CSV file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Iterator<CSVRecord> records, Classification classific, MathTransform reproject, int targetSRID, String outputFile) 
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
		        ++numRec;
		        
	            //Skip transformation of any features filtered out by the logical expression over thematic attributes
	            if (myAssistant.filterThematic(rs.toMap()))   //Custom thematic filtering is case sensitive and is using the original attribute names
	            {
			    	rejectedRec++;         
					continue;
			    }
	            
		      	//Pass all attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
		      	for (int i=0; i<rs.size(); i++)
		      	{
		      		//Names of attributes are case-sensitive in RML mappings; by convention, they must be written in upper case
		      		if (rs.get(i) != null)
		      		{
		      			row.put(csvHeader[i].toUpperCase(), rs.get(i));
		      			updateStatistics(csvHeader[i].toUpperCase());   //Update count of NOT NULL values transformed for this record
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
		      		//Apply spatial filtering (if specified by user)
					if (!myAssistant.filterContains(wkt))
					{
						rejectedRec++;
						continue;
					}
					//CRS transformation
			      	if (reproject != null)
			      		wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
//					else	
//					    myAssistant.WKT2Geometry(wkt);                      //This is done only for updating the MBR of all geometries
		
			  	  	//Insert extra attributes concerning lon/lat coordinates for the centroid 
			  	  	Geometry geomProjected = myAssistant.geomTransformWGS84(wkt, targetSRID);
			  	  	updateMBR(geomProjected);                 //Keep the MBR of transformed geometries up-to-date
			  	  	
		      		row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Extra attribute for the geometry as WKT
		      	}

		      	//Include a category identifier, as found in the classification scheme
		      	if (classific != null)
		      	{
		      		row.put("CATEGORY_URI", classific.getUUID(rs.get(currentConfig.attrCategory)));
		      		//Also determine the name of the embedded category assigned in the default classification scheme
					if (attrAssignedCategory != null)
						row.put(attrAssignedCategory, classific.getEmbeddedCategory(rs.get(currentConfig.attrCategory)));
		      	}
		      	
		      	//CAUTION! Also include a UUID as a 128-bit string that will become the basis for the URI assigned to the resulting triples
		      	String uri = myAssistant.getUUID(rs.get(currentConfig.attrKey)).toString();
		      	row.put("UUID", uri);
		      	
		        //Apply the transformation according to the given RML mapping		      
		        this.parseWithRML(row, dataset);
			  
				//Get a record with basic attribute that will be used for the SLIPO Registry
				if (myRegister != null)
					myRegister.createTuple(templateFeatureURI.replace("{UUID}", uri), row, wkt, targetSRID);			
				
			    //Periodically, dump results into output file
				if (numRec % currentConfig.batch_size == 0) 
				{
					numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!	
					collectTuples4Registry();                  //Basic attributes written to a .CSV for the SLIPO Registry
					myAssistant.notifyProgress(numRec);
				}
			}	
			
			//Dump any pending results into output file
			numTriples += this.writeTriples(dataset, writer, rdfFormat, "UTF-8");
			collectTuples4Registry();              //Also, basic attributes are written to a .CSV for the SLIPO Registry
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.abort(e, "Please check RML mappings.");
		}
		finally {
			store(outputFile);     //Dump any pending results into output file (under RML mode, this is used for the SLIPO Registry only)
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, rejectedRec, numTriples, currentConfig.serialization, getStatistics(), getMBR(), currentConfig.mode, currentConfig.targetCRS, outputFile, 0); 
	}
	

	/**
	 * Parses a single OSM record and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping.
	 * Input provided as an individual record (used with input format: OpenStreetMap XML).
	 * TODO: Implement for RML transformation mode.
	 */
	public void parse(OSMRecord rs, Classification classific, MathTransform reproject, int targetSRID) {
	
	}
			
	/**
	 * Parses a single GPX waypoint/track or a single JSON node and streamlines the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping.
	 * Input provided as an individual record (used with input format: GPX, JSON).
	 * TODO: Implement for RML transformation mode.
	 */
	public void parse(String wkt, Map<String, String> attrValues, Classification classific, int targetSRID, String geomType) {
		
	}

	/**
	 * Parses a Map structure of (key, value) pairs and streamlines the resulting triples (including geometric and non-spatial attributes).
	 * Input provided as an individual record. This method may be used when running over Spark/GeoSpark.
	 * TODO: Implement for RML transformation mode.
	 */
	public void parse(String wkt, Map<String,String> attrValues, Classification classific, int targetSRID, MathTransform reproject, String geomType, int partition_index, String outputFile) {

	}
	
	/**
	 * Collects tuples generated from a batch of features (their basic thematic attributes and their geometries) and streamlines them to a file for the SLIPO Registry.
	 */
	private void collectTuples4Registry() {
		try {
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
		} catch (IOException e) {
			ExceptionHandler.warn(e, "An error occurred during writing transformed record to the registry.");
		}
	}
	
	/**
	 * Stores resulting tuples into a file for the SLIPO Registry.	
	 * Used only for flushing data to the registry in RML transformation mode.
	 */	  
	public void store(String outputFile) {
  
		//******************************************************************
		//Close the file that will collect all tuples for the SLIPO Registry
		try {
			if (registryWriter != null)
				registryWriter.close();
		} catch (IOException e) {
			ExceptionHandler.abort(e, "An error occurred during creation of the file for the registry.");
		}
		//******************************************************************
	}
			
	/**
	 * Finalizes storage of resulting tuples into a file. This method is used when running over Spark/GeoSpark.
	 * Not applicable in RML transformation mode.
	 */
	public void store(String outputFile, int partition_index) {

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
	 * Provides the URI template used for all subjects in RDF triples concerning features.
	 * Applicable in RML transformation mode.
	 * @return A URI to be used as template for triples of features.
	 */	
	public String getURItemplate4Feature()
	{
		return templateFeatureURI;
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
