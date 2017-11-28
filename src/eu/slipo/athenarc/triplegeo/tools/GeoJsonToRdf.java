/*
 * @(#) GeoJsonToRdf.java	version 1.3   28/11/2017
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.model.dataset.StdRMLDataset;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Entry point to convert GeoJSON files into RDF triples.
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 11/4/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 7/11/2017, fixed issue with multiple instances of CRS factory
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */
public class GeoJsonToRdf {
	
//	  static final Logger LOG = Logger.getLogger(GeoJsonToRdf.class.getName());

	  Converter myConverter;
	  Assistant myAssistant;
	  MathTransform transform = null;
	  public int sourceSRID;                 //Source CRS according to EPSG 
	  public int targetSRID;                 //Target CRS according to EPSG 
	  public Configuration currentConfig;    //User-specified configuration settings
	  private Classification classification; //Classification hierarchy for assigning categories to features
	  public String inputFile;               //Input GeoJSON file
	  public String outputFile;              //Output RDF file
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	  //Class constructor
	  public GeoJsonToRdf(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  this.currentConfig = config;
		  this.classification = classific;
		  this.inputFile = inFile;
		  this.outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      myAssistant = new Assistant();
		   	  
		  //System.out.println("GeoJsonToRdf conversion from " + inputFile + " to " + outputFile);
		  
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.sourceCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);	  	        
	  		} catch (Exception e) {
	  			ExceptionHandler.invoke(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
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
		  //Apply transformation according to configuration settings
		  try { 
				//Collect results from the GeoJSON file
				FeatureCollection<?, ?> rs = collectData(inputFile);
				
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
				  else
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
   * Loads the GeoJSON file from the configuration path and returns the
   * feature collection associated according to the configuration.
   *
   * @param jsonPath with the path to the GeoJSON file.
   *
   * @return FeatureCollection with the collection of features filtered.
   */
  private static FeatureCollection<?, ?> collectData(String jsonPath)
  {	
	  FeatureCollection<?, ?> fc = null;
	  try
	  {
        InputStream in = new FileInputStream(jsonPath);
        FeatureJSON fJSON = new FeatureJSON();     
        fc = fJSON.readFeatureCollection(in);
		in.close();
	  } 
	  catch (Exception e) {
  			ExceptionHandler.invoke(e, "Cannot access input file.");      //Execution terminated abnormally
  	  }
        	
      return fc;
  }
  
  
  /**
   * 
   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
   */
private void executeParser4Graph(FeatureCollection<?, ?> featureCollection) throws UnsupportedEncodingException, FileNotFoundException 
  {
    FeatureIterator<?> iterator = featureCollection.features();
    SimpleFeatureImpl feature;
    Geometry geometry;
    String wkt;
    
    int numRec = 0;
    long t_start = System.currentTimeMillis();
    long dt = 0;
    
    try 
    {
      //System.out.println(myAssistant.getGMTime() + " Started processing features...");
 
      while(iterator.hasNext()) 
      {
        feature = (SimpleFeatureImpl) iterator.next();
        geometry = (Geometry) feature.getDefaultGeometry();

		//CRS transformation
      	if (transform != null)
      		geometry = myAssistant.geomTransform(geometry, transform);     
        
        //Get WKT representation of the transformed geometry
      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetOntology.trim());
        	
        //Parse geometric representation (including encoding to the target CRS)
        myConverter.parseGeom2RDF(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), wkt, targetSRID);
        
        //Process non-spatial attributes for name and type
        //CAUTION! Special handling of NULL values in GeoJSON
        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(),
        		((feature.getAttribute(currentConfig.attrName) != null) ? feature.getAttribute(currentConfig.attrName).toString() : ""), 
        		((feature.getAttribute(currentConfig.attrCategory) != null) ? feature.getAttribute(currentConfig.attrCategory).toString() : ""));
        
        myAssistant.notifyProgress(++numRec);

      }
    }
    catch (Exception e) {
    	ExceptionHandler.invoke(e, "An error occurred during transformation of an input record.");
    }
    finally {
      iterator.close();
    }
    
    dt = System.currentTimeMillis() - t_start;
    System.out.println(myAssistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
    System.out.println(myAssistant.getGMTime() + " Starting to write triples to file...");
    
    //Fetch resulting triples as stored in the model
    //model = myConverter.getModel();
    
    //Count the number of statements
    int numStmt = myConverter.getModel().listStatements().toSet().size();
    
    //Export model to a suitable format
    FileOutputStream out = new FileOutputStream(outputFile);
    myConverter.getModel().write(out, currentConfig.serialization);
    
    dt = System.currentTimeMillis() - t_start;
    myAssistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, outputFile);
  }


  /**
   * 
   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
   */
  private void executeParser4Stream(FeatureCollection<?, ?> featureCollection) throws Exception {
	  
	  	FeatureIterator<?> iterator = featureCollection.features();
	    SimpleFeatureImpl feature;
	    Geometry geometry;
	    String wkt;
	    
	    int numRec = 0;
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	    
	    
	  int numTriples = 0;
	    
	  OutputStream outFile = null;
	  try {
			outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
	  } 
	  catch (FileNotFoundException e) {
			e.printStackTrace();
	  } 
	  
	  //CAUTION! Hard constraint: serialization into N-TRIPLES is only supported by Jena riot (stream) interface
	  StreamRDF stream = StreamRDFWriter.getWriterStream(outFile, RDFFormat.NTRIPLES);   //StreamRDFWriter.getWriterStream(os, Lang.NT);	
	  stream.start();             //Start issuing streaming triples
	  
	try 
	{
	  //System.out.println(myAssistant.getGMTime() + " Started processing features...");
	 
	  //Iterate through all features
	  while(iterator.hasNext()) {
		  
		myConverter =  new StreamConverter(currentConfig);
		  
	    feature = (SimpleFeatureImpl) iterator.next();
	    geometry = (Geometry) feature.getDefaultGeometry();
	
		//CRS transformation
      	if (transform != null)
      		geometry = myAssistant.geomTransform(geometry, transform);     
        
        //Get WKT representation of the transformed geometry
      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetOntology.trim());
	
        //Parse geometric representation (including encoding to the target CRS)
        myConverter.parseGeom2RDF(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), wkt, targetSRID);
        
        //Process non-spatial attributes for name and type
        //CAUTION! Special handling of NULL values in GeoJSON
        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(),
        		((feature.getAttribute(currentConfig.attrName) != null) ? feature.getAttribute(currentConfig.attrName).toString() : ""), 
        		((feature.getAttribute(currentConfig.attrCategory) != null) ? feature.getAttribute(currentConfig.attrCategory).toString() : ""));
             
		//Append each triple to the output stream 
		for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
			stream.triple(myConverter.getTriples().get(i));
			numTriples++;
		}
		
		myAssistant.notifyProgress(++numRec);
	    
	  }
	}
    catch (Exception e) {
    	ExceptionHandler.invoke(e, "An error occurred during transformation of an input record.");
    }
	finally {
	  iterator.close();
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
  private void executeParser4RML(FeatureCollection<?, ?> featureCollection) {

	FeatureIterator<?> iterator = featureCollection.features();
    SimpleFeatureImpl feature;
    Geometry geometry;
    String wkt = "";
    
	//Determine the attribute schema of these features  
	final SimpleFeatureType original = (SimpleFeatureType) featureCollection.getSchema();
	List<String> attrNames = new ArrayList<String>();
	for(AttributeDescriptor ad : original.getAttributeDescriptors()) 
	{
	    final String name = ad.getLocalName();
	    if(!"boundedBy".equals(name) && !"metadataProperty".equals(name)) 
	    	attrNames.add(name);
	}
    
    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
    long t_start = System.currentTimeMillis();
    long dt = 0;
   
    int numRec = 0;
    int numTriples = 0;

    RMLDataset dataset = new StdRMLDataset();
    
    //Determine the serialization to be applied for the output triples
    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
    	  
	try {
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		
		//Iterate through all features
		while(iterator.hasNext()) {
  
			feature = (SimpleFeatureImpl) iterator.next();
			geometry = (Geometry) feature.getDefaultGeometry();
			  
			//CRS transformation
	      	if (transform != null)
	      		geometry = myAssistant.geomTransform(geometry, transform);     
	        
	        //Get WKT representation of the transformed geometry
	      	wkt = myAssistant.geometry2WKT(geometry, currentConfig.targetOntology.trim());
	      	
	      	//Pass all NOT NULL attribute values into a hash map in order to apply RML mapping(s) directly
	      	HashMap<String, String> row = new HashMap<>();	
	      	for (int i=0; i<feature.getNumberOfAttributes(); i++)
	      	{
	      		if ((!attrNames.get(i).equalsIgnoreCase(currentConfig.attrGeometry)) && (feature.getAttribute(i) != null))
	      		{
	      		    //Names of attributes in upper case; case-sensitive in RML mappings!
	      			row.put(attrNames.get(i).toUpperCase(), myAssistant.encodeUTF(feature.getAttribute(i).toString(), currentConfig.encoding));
	      			//System.out.println(attrNames.get(i) + " ");
	      		}
	      	}
	      	//System.out.println("");
	        row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   
	        //System.out.println(wkt);

	      	//Include a category identifier, as found in the classification scheme
	      	if (classification != null)
	      		row.put("CATEGORY_URI", classification.getURI(feature.getAttribute(currentConfig.attrCategory).toString()));
	      	//System.out.println(classification.getURI(rs.getString(currentConfig.attrCategory)));
	      	
	        //Apply the transformation according to the given RML mapping		      
	        myConverter.parseWithRML(row, dataset);
	            
	        //System.out.println(row.toString());
	        //Apply the transformation according to the given RML mapping		      
	        myConverter.parseWithRML(row, dataset);
	       
	        myAssistant.notifyProgress(++numRec);
		  
		    //Periodically, dump results into output file
			if (numRec % 1000 == 0) 
			{
				numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
				dataset = new StdRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
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
