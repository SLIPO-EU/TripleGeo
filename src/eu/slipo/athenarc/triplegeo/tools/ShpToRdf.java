/*
 * @(#) ShpToRdf.java	version 1.2   19/4/2017
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
package eu.slipo.athenarc.triplegeo.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
//import java.util.logging.Level;
//import java.util.logging.Logger;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;



/**
 * Entry point to convert shapefiles into RDF triples.
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 8/2/2013
 * Last modified by: Kostas Patroumpas, 19/4/2017
 */
public class ShpToRdf {
	
//	  static final Logger LOG = Logger.getLogger(ShpToRdf.class.getName());

	  Converter myConverter;
	  MathTransform transform = null;
	  public int sourceSRID;               //Source CRS according to EPSG 
	  public int targetSRID;               //Target CRS according to EPSG 
	  public Configuration currentConfig;  //User-specified configuration settings
	  public String inputFile;             //Input shapefile
	  public String outputFile;            //Output RDF file
	  
	
	  //Class constructor
	  public ShpToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  this.currentConfig = config;
		  this.inputFile = inFile;
		  this.outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
		   	  
		  //System.out.println("ShpToRdf conversion from " + inputFile + " to " + outputFile);
		  
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.sourceCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        
	  	        Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
	  	        CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
	  	        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);	  	        
	  		} catch (Exception e) {
	  			e.printStackTrace();
	  		}
		 	    
		  // Other parameters
		  if (Assistant.isNullOrEmpty(currentConfig.defaultLang)) {
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
				//Collect results from the shapefile
				FeatureCollection<?, ?> rs = collectData(inputFile);
				
				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig);
				
				  //Export data after constructing a model on disk
				  executeParser4Graph(rs);
				  
				  //Remove all temporary files as soon as processing is finished
				  Assistant.removeDirectory(currentConfig.tmpDir);
				}
				else
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4Stream(rs);
				}
	  		} catch (Exception e) {
	  			e.printStackTrace();
	  		}
	
	}
	
  
  /**
   * Loads the shape file from the configuration path and returns the
   * feature collection associated according to the configuration.
   *
   * @param shapePath with the path to the shapefile.
   *
   * @return FeatureCollection with the collection of features filtered.
   */
  private static FeatureCollection<?, ?> collectData(String shapePath) throws IOException 
  {
	    File file = new File(shapePath);
	    DataStore dataStore = null;
	    
	    // Create the map with the file URL to be passed to DataStore.
	    Map<String,URL> map = new HashMap<String, URL>();
	    try {
	    	map.put("url", file.toURI().toURL());
		    if (map.size() > 0) {
		    	dataStore = DataStoreFinder.getDataStore(map);
		    	FeatureSource<?, ?> featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
		    	return featureSource.getFeatures();
		    }
	    } catch (MalformedURLException e) {
	    	e.printStackTrace();
	    	//Logger.getLogger(ShpToRdf.class.getName()).log(Level.SEVERE, null, e);
	    }
	    finally {
	    	dataStore.dispose();
	    }
	    
	    return null;
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
    WKTWriter writer = new WKTWriter();
    String wkt;
    
    int numRec = 0;
    long t_start = System.currentTimeMillis();
    long dt = 0;
    
    try 
    {
      //System.out.println(Assistant.getGMTime() + " Started processing features...");
 
      while(iterator.hasNext()) {
        feature = (SimpleFeatureImpl) iterator.next();
        geometry = (Geometry) feature.getDefaultGeometry();

        //Attempt to transform geometry into the target CRS
        if (transform != null)
	        try {
				geometry = JTS.transform(geometry, transform);
			} catch (Exception e) {
				e.printStackTrace();
			}

        //Convert geometry into WKT
        if (currentConfig.targetOntology.trim().toLowerCase().equals("geosparql"))
        	wkt = writer.writeFormatted(geometry);        //GeoSPARQL WKT representation for various types of geometries
        else
        	wkt = geometry.toText();                      //WKT representation for Virtuoso or WGS84 GeoPosition RDF vocabulary
        	
        //Parse geometric representation (including encoding to the target CRS)
        myConverter.parseGeom2RDF(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), wkt, targetSRID);
        
        //Process non-spatial attributes for name and type
        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), feature.getAttribute(currentConfig.attrName).toString(), feature.getAttribute(currentConfig.attrCategory).toString());
             
        Assistant.notifyProgress(++numRec);

      }
    }
    finally {
      iterator.close();
    }
    
    dt = System.currentTimeMillis() - t_start;
    System.out.println(Assistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
    System.out.println(Assistant.getGMTime() + " Starting to write triples to file...");
    
    //Fetch resulting triples as stored in the model
    //model = myConverter.getModel();
    
    //Count the number of statements
    int numStmt = myConverter.getModel().listStatements().toSet().size();
    
    //Export model to a suitable format
    FileOutputStream out = new FileOutputStream(outputFile);
    myConverter.getModel().write(out, currentConfig.serialization);
    
    dt = System.currentTimeMillis() - t_start;
    Assistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, outputFile);
  }


  /**
   * 
   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
   */
  private void executeParser4Stream(FeatureCollection<?, ?> featureCollection) throws Exception {
  
	  FeatureIterator<?> iterator = featureCollection.features();
	    SimpleFeatureImpl feature;
	    Geometry geometry;
	    WKTWriter writer = new WKTWriter();
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
	  //System.out.println(Assistant.getGMTime() + " Started processing features...");
	 
	  //Iterate through all features
	  while(iterator.hasNext()) {
		  
		myConverter =  new StreamConverter(currentConfig);
		  
	    feature = (SimpleFeatureImpl) iterator.next();
	    geometry = (Geometry) feature.getDefaultGeometry();
	
	    //Attempt to transform geometry into the target CRS
	    if (transform != null)
	        try {
				geometry = JTS.transform(geometry, transform);
			} catch (Exception e) {
				e.printStackTrace();
			}
	
	    //Convert geometry into WKT
	    if (currentConfig.targetOntology.trim().toLowerCase().equals("geosparql"))
	    	wkt = writer.writeFormatted(geometry);        //GeoSPARQL WKT representation for various types of geometries
	    else
	    	wkt = geometry.toText();                      //WKT representation for Virtuoso or WGS84 Geoposition RDF vocabulary
	
        //Parse geometric representation (including encoding to the target CRS)
        myConverter.parseGeom2RDF(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), wkt, targetSRID);
        
        //Process non-spatial attributes for name and type
        if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
        	myConverter.handleNonGeometricAttributes(currentConfig.featureName, feature.getAttribute(currentConfig.attrKey).toString(), feature.getAttribute(currentConfig.attrName).toString(), feature.getAttribute(currentConfig.attrCategory).toString());
             
		//Append each triple to the output stream 
		for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
			stream.triple(myConverter.getTriples().get(i));
			numTriples++;
		}
		
		Assistant.notifyProgress(++numRec);
	    
	  }
	}
	finally {
	  iterator.close();
	}

	stream.finish();               //Finished issuing triples
	
    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    Assistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);

  }

}
