/*
 * @(#) Extractor.java	version 1.5  10/10/2018
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
package eu.slipo.athenarc.triplegeo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FilenameUtils;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;


/**
 * Entry point to TripleGeo for converting from various input formats (MULTI-THREADED EXECUTION)
 * @author Kostas Patroumpas
 * @version 1.6
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 23/3/2017
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Modified: 8/11/2017; added support for preparing a classification scheme to be applied over entities
 * Modified: 21/11/2017; handling missing specifications for classification and RML mapping files
 * Modified: 12/2/2018; handling missing specifications on georeferencing (CRS: Coordinate Reference Systems) 
 * Modified: 13/7/2018; advanced handling of interrupted or aborted tasks
 * Last modified: 10/10/2018
 */
public class Extractor {

	static Assistant myAssistant;
	private static Configuration currentConfig;         //Configuration settings for the transformation
	static Classification classification = null;        //Classification hierarchy for assigning categories to features
	static String[] inputFiles;
	static List<String> outputFiles;
	static int sourceSRID;                              //Source CRS according to EPSG 
	static int targetSRID;                              //Target CRS according to EPSG

	/**
	 * Main entry point to execute the transformation process.
	 * @param args  Arguments for the execution, including the path to a configuration file.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
//	@SuppressWarnings("unused")
	public static void main(String[] args)  { 

		System.out.println(Constants.COPYRIGHT);
		
	    List<Future<Task>> runnables;                  //List of transformation tasks to be executed by concurrent threads
	    boolean failure = false;                       //Indicates whether at least one task has failed to conclude
	    
	    if (args.length >= 0)  {

	    	myAssistant =  new Assistant();
	    	
	    	//Specify a configuration file with properties used in the conversion
	    	currentConfig = new Configuration(args[0]);          //Argument like "./bin/shp_options.conf"
	    	
	    	//Conversion mode: (in-memory) STREAM or (disk-based) GRAPH or through RML mappings or via XSLT transformation
			System.out.println("Conversion mode: " + currentConfig.mode);
			
			//Issue copyright statement for RML processing modules
			if (currentConfig.mode.contains("RML"))
				System.out.println(Constants.RML_COPYRIGHT);
				
			//Force N-TRIPLES serialization in case that the STREAM mode is chosen
			if (currentConfig.mode.contains("STREAM"))
				currentConfig.serialization = "N-TRIPLES";
			
			//Force RDF/XML serialization in case that the XSLT mode is chosen (for XML/GML input)
			if (currentConfig.mode.contains("XSLT"))
				currentConfig.serialization = "RDF/XML";
			
			//Clean up temporary directory to be used in transformation when disk-based GRAPH mode is chosen
			if (currentConfig.mode.contains("GRAPH"))
				myAssistant.removeDirectory(currentConfig.tmpDir);
				
			System.out.println("Output serialization: " + currentConfig.serialization);
			
			System.setProperty("org.geotools.referencing.forceXY", "true");
			
			//Check how many input files have been specified
			if (currentConfig.inputFiles != null)
			{
				inputFiles = currentConfig.inputFiles.split(";");     //MULTIPLE input file names separated by ;
			}
			else   //This is the case that a SINGLE input comes from a DBMS
			{      //Workaround: The (virtual) input file is considered to have the same name as the original table
				inputFiles = new String [] {currentConfig.outputDir + currentConfig.tableName + ".rdf"};
			}			

			//Initialize collection of output files that will carry the resulting RDF triples
			outputFiles = new ArrayList<String>();
			
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
			 
			//Check whether a classification hierarchy is specified in a separate file and apply transformation accordingly
	        try { 			  
				if ((currentConfig.classificationSpec != null) && (!currentConfig.inputFormat.contains("OSM")))    //Classification for OSM XML data is handled by the OSM converter
				{
					String outClassificationFile = currentConfig.outputDir + FilenameUtils.getBaseName(currentConfig.classificationSpec) + myAssistant.getOutputExtension(currentConfig.serialization);
					classification = new Classification(currentConfig, currentConfig.classificationSpec, outClassificationFile);  
					outputFiles.add(outClassificationFile);
				} 
	        }  
	     	//Handle any errors that may have occurred during reading of classification hierarchy  
	        catch (Exception e) {  
	        	ExceptionHandler.abort(e, "Failed to read classification hierarchy.");  
	     	} 
	        finally {
	        	if (classification != null)
	        		System.out.println(myAssistant.getGMTime() + " Classification hierarchy read successfully!");
	        	else if (!currentConfig.inputFormat.contains("OSM"))
	        		System.out.println("No classification hierarchy specified for features to be extracted.");
	        }
	        
		    //Possible executors with different behaviour
		    ExecutorService exec = Executors.newCachedThreadPool();        //Create new threads dynamically as needed at runtime
		    //ExecutorService exec = Executors.newFixedThreadPool(3);      //NOT USED: Predetermined number of threads to be used at runtime
		    //ExecutorService exec = Executors.newSingleThreadExecutor();  //NOT USED: Single-thread execution, i.e., one task after the other
	    
		    //Create a list of all tasks to be executed with their respective input and output files, but with the same transformation settings
		    //The number of tasks is equal to the number of input files specified in the configuration file
		    List<Callable<Task>> tasks = new ArrayList<Callable<Task>>();
		    for (final String inFile: inputFiles) {
		    	//CAUTION! An output file will be named as its corresponding input file, but with a different extension (auto-specified by the RDF serialization format)
		    	outputFiles.add(currentConfig.outputDir + FilenameUtils.getBaseName(inFile) + myAssistant.getOutputExtension(currentConfig.serialization));
	        	Callable<Task> c = new Callable<Task>() {
	        		final String outFile = outputFiles.get(outputFiles.size()-1); 
	        		@Override
		        	public Task call() throws Exception {
		            	return new Task(currentConfig, classification, inFile, outFile, sourceSRID, targetSRID);
		            }
		        };
		        tasks.add(c);
		    }

		    long start = System.currentTimeMillis();
		    //Invoke all the tasks concurrently
		    try {
		    	System.out.print(myAssistant.getGMTime() + " Started processing features ");
		    	if (tasks.size() > 1)
		    		System.out.println("using " + tasks.size() + " concurrent threads...");
		    	else
		    		System.out.println("in a single thread...");
		        runnables = exec.invokeAll(tasks);	
		        
		        //Inspect each task on possible failure
		        for (Future<Task> r : runnables) {
		        	try {
						if (r.get() == null)
							failure = true;             //At least one task has failed
					} catch (ExecutionException e) {
						failure = true;
						ExceptionHandler.warn(e, "A transformation task failed.");            //Execution aborted abnormally
					}
		        	catch (InterruptedException e) {
		        		failure = true;
		        		ExceptionHandler.warn(e, "A transformation task was interrupted.");   //Execution interrupted abnormally
				    }
		        }
		    }		     
		    catch(Exception e) {
		    	ExceptionHandler.abort(e, "A transformation task failed.");      //Execution terminated abnormally
		    }
		    finally {
		        exec.shutdown();
		        long elapsed = System.currentTimeMillis() - start;
		        if (failure) {
		        	System.out.println(myAssistant.getGMTime() + String.format(" Transformation process failed. Elapsed time: %d ms.", elapsed));
		        	System.exit(1);          //Execution failed in at least one task
		        }
		        else {
			        System.out.println(myAssistant.getGMTime() + String.format(" Transformation process concluded successfully in %d ms.", elapsed));
			        System.out.println("RDF results written into the following output files:" + outputFiles.toString());
			        //Assistant.mergeFiles(outputFiles, "C:/Development/Java/workspace/TripleGeo/test/output/merged_output.rdf");
			        System.exit(0);          //Execution completed successfully
		        }
		    }
		    
	    } 
	    else {
			System.err.println(Constants.INCORRECT_CONFIG); 
			System.exit(1);          //Execution terminated abnormally
	    }		    	 			    
	  }

}