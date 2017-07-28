/*
 * @(#) Executor.java	version 1.2   11/4/2017
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
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;


/**
 * Entry point to TripleGeo for converting from various input formats (MULTI-THREADED EXECUTION)
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 23/3/2017
 * Last modified by: Kostas Patroumpas, 11/4/2017
 */
public class Executor {

	private static Configuration currentConfig;
	static String[] inputFiles;
	static int sourceSRID;                //Source CRS according to EPSG 
	static int targetSRID;                //Target CRS according to EPSG

	@SuppressWarnings("unused")
	public static void main(String[] args) throws InterruptedException, ExecutionException {

		System.out.println(Constants.COPYRIGHT);
		
	    if (args.length >= 0)  {

	    	//Specify a configuration file with properties used in the conversion
	    	currentConfig = new Configuration(args[0]);          //Argument like "./bin/shp_options.conf"
	    	
	    	//Conversion mode: (in-memory) STREAM or (disk-based) GRAPH
			System.out.println("Conversion mode: " + currentConfig.mode);
			
			//Force N-TRIPLES serialization in case that the STREAM mode is chosen
			if (currentConfig.mode.contains("STREAM"))
				currentConfig.serialization = "N-TRIPLES";
			
			System.out.println("Output serialization: " + currentConfig.serialization);
			
			System.setProperty("org.geotools.referencing.forceXY", "true");
			
			//Check how many input files have been specified
			if (currentConfig.inputFiles != null)
			{
				inputFiles = currentConfig.inputFiles.split(";");     //MULTIPLE input files separated by ;
			}
			else   //This is the case that a SINGLE input comes from a DBMS
			{      //Workaround: The (virtual) input file will have the same name as the original table
				inputFiles = new String [] {currentConfig.outputDir + currentConfig.tableName + ".rdf"};
			}			

			//Check if a coordinate transform is required for geometries
			  if (currentConfig.sourceCRS != null)
			    try {
			        //Needed for parsing original geometry in WTK representation
			    	sourceSRID = Integer.parseInt(currentConfig.sourceCRS.substring( currentConfig.sourceCRS.indexOf(':')+1, currentConfig.sourceCRS.length()));
			    	targetSRID = Integer.parseInt(currentConfig.targetCRS.substring( currentConfig.targetCRS.indexOf(':')+1, currentConfig.targetCRS.length()));
			    	System.out.println("Transformation will take place from " + currentConfig.sourceCRS + " to " + currentConfig.targetCRS + " reference system.");
			        
				} catch (Exception e) {
					e.printStackTrace();
				}
			  else   
			  {   //No transformation will take place; all features assumed in WGS84 lon/lat coordinates
				  sourceSRID = 4326;           //WGS84
				  targetSRID = 4326;           //WGS84
				  System.out.println(Constants.NO_REPROJECTION);
			  }
		      
		    //Possible executors with different behaviour
		    ExecutorService exec = Executors.newCachedThreadPool();        //Create new threads dynamically as needed at runtime
		    //ExecutorService exec = Executors.newFixedThreadPool(3);      //Predetermined number of threads to be used at runtime
		    //ExecutorService exec = Executors.newSingleThreadExecutor();  //Single-thread execution, i.e., one task after the other
	    
		    //Create a list of all tasks to be executed with their respective input and output files, but with the same transformation settings
		    //The number of tasks is equal to the number of input files specified in the configuration file
		    List<Callable<Task>> tasks = new ArrayList<Callable<Task>>();
		    for (final String inFile: inputFiles) {
	        	Callable<Task> c = new Callable<Task>() {
	        		final String outFile = currentConfig.outputDir + FilenameUtils.getBaseName(inFile) + Assistant.getOutputExtension(currentConfig.serialization);		        
	        		@Override
		        	public Task call() throws Exception {
		            	return new Task(currentConfig, inFile, outFile, sourceSRID, targetSRID);
		            }
		        };
		        tasks.add(c);
		    }

		    //Invoke all the tasks concurrently
		    try {
		    	System.out.println(Assistant.getGMTime() + " Started processing features...");
		        long start = System.currentTimeMillis();
		        List<Future<Task>> results = exec.invokeAll(tasks);
		        long elapsed = System.currentTimeMillis() - start;
		        System.out.println(Assistant.getGMTime() + String.format(" Transformation process concluded in %d ms.", elapsed));
		    } finally {
		        exec.shutdown();		        
		    }
		    
	    } 
	    else {
			System.err.println(Constants.INCORRECT_CONFIG); 
	    }		    	 			    
	  }

}
