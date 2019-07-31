/*
 * @(#) ShpToRdf.java	version 1.9   12/7/2019
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
package eu.slipo.athenarc.triplegeo.tools;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Entry point to convert ESRI shapefiles into RDF triples.
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 8/2/2013
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 7/11/2017, fixed issue with multiple instances of CRS factory
 * Modified: 21/11/2017, added support for recognizing a user-specified classification scheme (RML mode only)
 * Modified: 12/12/2017, fixed issue with string encodings; verified that UTF characters read and written correctly
 * Modified: 13/12/2017, utilizing a streaming iterator in order to avoid loading the entire feature collection into memory
 * Modified: 12/7/2018, checking availability of basic shapefile components before starting any processing
 * Modified: 19/4/2019, included support for spatial filtering over the input shapefile
 * Last modified by: Kostas Patroumpas, 12/7/2019
 */
public class ShpToRdf {
	
	  Converter myConverter;
	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  int sourceSRID;                            //Source CRS according to EPSG 
	  int targetSRID;                            //Target CRS according to EPSG 
	  private Configuration currentConfig;       //User-specified configuration settings
	  private Classification classification;     //Classification hierarchy for assigning categories to features
	  String inputFile;                          //Input shapefile
	  String outputFile;                         //Output RDF file
	  DataStore dataStore = null;                //Data store used for accessing the shapefile
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
  
	  /**
	   * Constructor for the transformation process from ESRI shapefile to RDF.
	   * @param config  Parameters to configure the transformation.
	   * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	   * @param inFile   Path to input shapefile.
	   * @param outFile  Path to the output file that collects RDF triples.
	   * @param sourceSRID  Spatial reference system (EPSG code) of the input shapefile.
	   * @param targetSRID Spatial reference system (EPSG code) of geometries in the output RDF triples.
	   * @throws ClassNotFoundException
	   */
	  public ShpToRdf(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  this.currentConfig = config;
		  this.classification = classific;
		  this.inputFile = inFile;
		  this.outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      myAssistant = new Assistant(config);

	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.targetCRS != null) 
	      {
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);  	        
	  		} catch (Exception e) {
	  			ExceptionHandler.abort(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
	  		}
	      }
	      else  //No transformation specified; determine the CRS of geometries
	      {
	    	  if (sourceSRID == 0)
	    		  this.targetSRID = 4326;          //All features assumed in WGS84 lon/lat coordinates
	    	  else
	    		  this.targetSRID = sourceSRID;    //Retain original CRS
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
			//Collect results from the shapefile
		    FeatureIterator<?> rs = collectData(inputFile);    // FeatureCollection<?, ?> rs = collectData(inputFile);
			
			if (currentConfig.mode.contains("GRAPH"))
			{
			  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
			  myConverter = new GraphConverter(currentConfig, myAssistant, outputFile);
			
			  //Export data after constructing a model on disk
			  myConverter.parse(rs, classification, reproject, targetSRID, outputFile);
			  
			  //Remove all temporary files as soon as processing is finished
			  myAssistant.removeDirectory(myConverter.getTDBDir());
			}
			else if (currentConfig.mode.contains("STREAM"))
			{
			  //Mode STREAM: consume records and streamline them into a serialization file
			  myConverter =  new StreamConverter(currentConfig, myAssistant, outputFile);
			  
			  //Export data in a streaming fashion
			  myConverter.parse(rs, classification, reproject, targetSRID, outputFile);
			}
			else if (currentConfig.mode.contains("RML"))
			{
			  //Mode RML: consume records and apply RML mappings in order to get triples
			  myConverter =  new RMLConverter(currentConfig, myAssistant);
			  
			  //Export data in a streaming fashion according to RML mappings
			  myConverter.parse(rs, classification, reproject, targetSRID, outputFile);
			}
  		} catch (Exception e) {
  			ExceptionHandler.abort(e, "");
  		}
	  	finally {
	  		dataStore.dispose();	  
	  	}
   }
	
  
  /**
   * Loads a shapefile from the configuration path and returns an iterable feature collection.
   * @param shapePath Specifies the path to the shapefile.
   * @return FeatureIterator over the collection of features that can be streamed into the transformation process.
   */
   private FeatureIterator<?> collectData(String shapePath) 
   {
	    File file = new File(shapePath);
	
	    //Specify parameters to be passed to DataStore
	    Map<String,Serializable> map = new HashMap<String, Serializable>();
	    try {
	    	map.put("url", file.toURI().toURL());
	    	map.put("charset", currentConfig.encoding);
		    if (map.size() > 0) 
		    {
		    	dataStore = DataStoreFinder.getDataStore(map);
		    	
		    	//Check whether the input shapefile is valid
		    	if (!validateShapefile(file, dataStore.getTypeNames()[0])) {
		    		System.out.println("ERROR! Shapefile " + shapePath + " is malformed and cannot be processed! Check if all its constituent files are available in the same folder.");
		    		throw new IOException();
		    	}
		    	
		    	//Create a feature source over this shapefile
		    	FeatureSource<?, ?> featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
		    		    	
		    	//In case no CRS transformation has been specified, keep resulting geometries in their original georeferencing
		    	if (targetSRID == 0)
		    	{   
		    		try {
		    			targetSRID = CRS.lookupEpsgCode(featureSource.getSchema().getCoordinateReferenceSystem(), true);
		    		}
		    		catch (Exception e) {
		    			ExceptionHandler.abort(e, "Cannot recognize the coordinate reference system used for the input data. Please specify this in the configuration settings.");
		    		}
		    	} 
	    	
		    	return featureSource.getFeatures().features();    //Actually returning an iterator over the feature collection
		    }
	    } catch (Exception e) {
	    	ExceptionHandler.abort(e, "Cannot access input file.");      //Execution terminated abnormally
	    	//Logger.getLogger(ShpToRdf.class.getName()).log(Level.SEVERE, null, e);
	    }
	    
	    return null;
   }

   /**
    * Checks whether a valid shapefile is given as input, i.e., at least the three .shp, .shx, .dbf files are available in the same folder 
    * @param shpPath  Path to the shapefile on disk.
    * @param shpName  Name of the shapefile (without extensions)
    * @return  True if the shapefile seems valid and can be processed; otherwise, False.
    */
   private boolean validateShapefile(File shpPath, String shpName)
   {
	   String dataDir = shpPath.getParent();
	   String[] fileExtensions = {".shp",".shx",".dbf"};      //Those three files are indispensable for any valid shapefile

	   for (String ext: fileExtensions) {
		   File f = new File(dataDir + "/" + shpName + ext);
		   if (!f.exists())
		       return false;
	   }

       return true;
   }
   
}
