/*
 * @(#) ReverseExtractor.java	version 2.0   31/10/2019
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
package eu.slipo.athenarc.triplegeo;

import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;

import eu.slipo.athenarc.triplegeo.tools.RdfToCsv;
import eu.slipo.athenarc.triplegeo.tools.RdfToGeoJson;
import eu.slipo.athenarc.triplegeo.tools.RdfToShp;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.ReverseConfiguration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.BatchReverseConverter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.ReverseConverter;


/**
 * Utility for validating RDF datasets and exporting them into geographical files (reverse transformation).
 * LIMITATIONS: Currently supporting CSV (delimited) files, ESRI Shapefiles, and GeoJson files.
 * USAGE: Execution command over JVM:
 *        java -cp target/triplegeo-2.0-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.ReverseExtractor  <path-to-configuration-file> 
 * ARGUMENTS: (1) Path to a properties file with the configuration to be used in the reverse transformation.
 * 
 * @author Kostas Patroumpas
 * @version 2.0
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/9/2017
 * Modified: 12/2/2018; handling missing specifications on georeferencing (CRS: Coordinate Reference Systems) 
 * Modified: 12/7/2019; handling also export to GeoJson files
 * Last modified by: Kostas Patroumpas, 31/10/2019
 */
public class ReverseExtractor {  

	static Assistant myAssistant;
	private static ReverseConfiguration currentConfig;
	static String[] inputFiles;                         //Path to a graph model that will be created on disk
	static BatchReverseConverter myReverseConverter;
	static String sparqlFile;                           //Path to file containing a SPARQL SELECT query over this graph
	static String outFile;                              //Path to the file where the reconverted data will be written
	static int sourceSRID;                				//Source CRS according to EPSG 
	static int targetSRID;                				//Target CRS according to EPS
	static ReverseConverter conv;                       //Instantiates a reverse converter (either for CSV or shapefile)
	
	/**
	 * Main entry point to execute the reverse transformation process.
	 * @param args Arguments for the execution, including the path to a configuration file.
	 */
    public static void main(String[] args) {  

	    System.out.println(Constants.COPYRIGHT);
	   
	    if (args.length >= 0)  {  //Takes as argument a configuration file for the reverse transformation (which also points to a file containing a SELECT query in SPARQL)

	    	myAssistant = new Assistant();
	    	
	    	//The only argument must be a configuration file with properties used in the reverse transformation
	    	currentConfig = new ReverseConfiguration(args[0]);          //Argument like "./test/shp_reverse_options.conf"	
	    	
	        try { 	
				//Check how many input files have been specified
				if (currentConfig.inputFiles != null)
					inputFiles = currentConfig.inputFiles.split(";");     //MULTIPLE input files must be separated by ;	in the configuration		

				if (currentConfig.sparqlFile != null)
					sparqlFile = currentConfig.sparqlFile;                //A file with a valid SELECT query specified in SPARQL
				else
				{
					System.err.println("No file specified in the configuration with a SPARQL query to be applied over the RDF data.");
					throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
				}
					
				//Check if a coordinate transform is required for geometries
				 try {
					 if (currentConfig.sourceCRS != null)
					 {
				        //Needed for parsing original geometry in WTK representation
				    	sourceSRID = Integer.parseInt(currentConfig.sourceCRS.substring( currentConfig.sourceCRS.indexOf(':')+1, currentConfig.sourceCRS.length()));
					 }
					 else 
						 sourceSRID = 0;                //Non-specified
					 
					 if (currentConfig.targetCRS != null)
					 {
				        //Needed for parsing original geometry in WTK representation
				    	targetSRID = Integer.parseInt(currentConfig.targetCRS.substring( currentConfig.targetCRS.indexOf(':')+1, currentConfig.targetCRS.length()));
				    	System.out.println("Transformation will take place from " + currentConfig.sourceCRS + " to " + currentConfig.targetCRS + " reference system.");	
					 }
					 else
					 {   //No transformation will take place
	                     //CAUTION! Not always safe to assume that features are georeferenced in WGS84 lon/lat coordinates 
						 targetSRID = 0; 				//Non-specified	     
						 System.out.println(Constants.NO_REPROJECTION);
					 }
				 } catch (Exception e) {
						ExceptionHandler.abort(e, "Please check SRID specifications in the configuration.");      //Execution terminated abnormally
				 }

				//Initialize the output container for the reconverted results
				try {
					String currentFormat = currentConfig.outputFormat.toUpperCase();     //Possible values: SHAPEFILE, CSV
					
					try {
						if (currentFormat.trim().contains("CSV")) 
						{
							outFile = currentConfig.outputFile;
							conv = new RdfToCsv(currentConfig, outFile, sourceSRID, targetSRID);
						}
						else if (currentFormat.trim().contains("SHAPEFILE")) 
						{	
							outFile = currentConfig.outputFile;
							conv = new RdfToShp(currentConfig, outFile, sourceSRID, targetSRID);
						}
						else if (currentFormat.trim().contains("GEOJSON")) 
						{	
							outFile = currentConfig.outputFile;
							conv = new RdfToGeoJson(currentConfig, outFile, sourceSRID, targetSRID);
						}
						else {
							throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
						}
						
					} catch (Exception e) {
			        	ExceptionHandler.abort(e, Constants.INCORRECT_SETTING);      //Execution terminated abnormally
					}					
				} catch(Exception e) {
					 ExceptionHandler.abort(e, "Instantiating a reverse converter failed.");	
				}
				
				try {
					//Remove any intermediate files possibly remained in the temporary directory after previous executions
					myAssistant.removeDirectory(currentConfig.tmpDir);
					  
		    	  	//Call reverse converter
		    	  	myReverseConverter =  new BatchReverseConverter(currentConfig, inputFiles);

					//Create the graph model
					myReverseConverter.createModel();
					
					//Read a valid SPARQL query from a file
					//CAUTION! This must be a valid SELECT query that reflects the schema and properties of the RDF graph
					FileInputStream inputStream = new FileInputStream(sparqlFile); 
					String q = IOUtils.toString(inputStream, "UTF-8");
					  
					//Submit query against this graph model and store results to the output file
					myReverseConverter.queryModel(q, conv);
					  
					//Finally, close the graph model
					myReverseConverter.closeModel();	
				  
					//... and the output file
					conv.close();
					
				} catch (Exception e) {
					ExceptionHandler.abort(e, "Failed to instantiate a reverse converter.");
		  	  	}
	      }  
	
	      // Handle any errors that may have occurred during reverse transformation  
	      catch (Exception e) {  
	         ExceptionHandler.abort(e, "Reverse transformation from RDF graph failed.");  
	      } 
		  finally {
			  System.out.println(myAssistant.getGMTime() + " Reverse transformation process terminated successfully!");
			  System.out.println("Records were written into output file:" + outFile.toString());
		  }
	    }
	    else {
			System.err.println(Constants.INCORRECT_CONFIG); 
			System.exit(1);          //Execution terminated abnormally
	    }
    }  
}  
