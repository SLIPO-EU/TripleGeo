/*
 * @(#) GeoJsonToRdf.java	version 1.8   19/7/2018
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

import org.geotools.factory.Hints;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Entry point to convert GeoJSON files into RDF triples.
 * LIMITATIONS: - Nested properties (non-spatial) in GeoJSON are considered as carrying NULL values by GeoTools.
 *              - Each feature must have the same properties, i.e., all features must comply with the same attribute schema. 
 * @author Kostas Patroumpas
 * @version 1.8
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 11/4/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 7/11/2017, fixed issue with multiple instances of CRS factory
 * Modified: 13/12/2017, utilizing a streaming iterator in order to avoid loading the entire feature collection into memory
 * TODO: Upgrade to newer GeoTools library for GeoJSON.
 * Last modified by: Kostas Patroumpas, 19/7/2018
 */
public class GeoJsonToRdf {

	  Converter myConverter;
	  Assistant myAssistant;
	  MathTransform reproject = null;
	  int sourceSRID;                        //Source CRS according to EPSG 
	  int targetSRID;                        //Target CRS according to EPSG 
	  Configuration currentConfig;           //User-specified configuration settings
	  private Classification classification; //Classification hierarchy for assigning categories to features
	  String inputFile;                      //Input GeoJSON file
	  String outputFile;                     //Output RDF file
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	  /**
	   * Constructor for the transformation process from GeoJSON file to RDF.
	   * @param config  Parameters to configure the transformation.
	   * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	   * @param inFile   Path to input GeoJSON file.
	   * @param outFile  Path to the output file that collects RDF triples.
	   * @param sourceSRID  Spatial reference system (EPSG code) of the input geometries.
	   * @param targetSRID Spatial reference system (EPSG code) of geometries in the output RDF triples.
	   * @throws ClassNotFoundException
	   */
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
			//Collect results from the GeoJSON file
			FeatureIterator<?> rs = collectData(inputFile);    // FeatureCollection<?, ?> rs = collectData(inputFile);
			
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
   * Opens the GeoJSON file specified in the configuration and returns a feature iterator.
   * @param jsonPath  The path to the GeoJSON file.
   * @return FeatureIterator over the collection of features that can be streamed into the transformation process.
   */
  private FeatureIterator<?> collectData(String jsonPath)
  {
	  FeatureIterator<?> iterator = null;
	  try
	  {
        FeatureJSON fJSON = new FeatureJSON(); 
        
        //In case no CRS transformation has been specified, assume that CRS for all GeoJSON coordinates is WGS84, equivalent to OGC URN urn:ogc:def:crs:OGC::CRS84.
    	if (sourceSRID == 0)	
    	{
    		targetSRID = 4326;
    		System.out.println(Constants.WGS84_PROJECTION);
    	}
    	
        iterator = fJSON.streamFeatureCollection(jsonPath);        
	  }
	  catch (Exception e) {
			ExceptionHandler.abort(e, "Cannot access input file.");      //Execution terminated abnormally
	  }
	  return iterator;
  }
  
 
}
