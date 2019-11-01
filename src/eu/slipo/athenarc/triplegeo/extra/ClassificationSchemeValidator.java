/*
 * @(#) ClassificationSchemeValidator.java	version 2.0   31/10/2019
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
package eu.slipo.athenarc.triplegeo.extra;

import java.util.Map;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Category;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * Auxiliary utility to TripleGeo for checking the validity a classification hierarchy stored in a file.
 * USAGE: Execution command over JVM:
 *        java -cp target/triplegeo-2.0-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.extra.ClassificationSchemeValidator <path-to-CSV-or-YML-classification-file> <boolean-flag> <output-CSV-or-YML-format>
 * ARGUMENTS: (1) Input file (CSV or YML) containing a user-specified classification scheme.
 *            (2) Boolean value: Is each category referenced by its identifier in the classification scheme (false) or by the actual name of the category (true)? 
 *            (3) Default format used in printing out the reconstructed classification (CSV or YML).
 * 
 * Created by: Kostas Patroumpas, 7/11/2017
 * Modified: 12/2/2018; support checking against multi-tier classification schemes
 * Last modified by: Kostas Patroumpas, 31/10/2019
 * 
 * @author Kostas Patroumpas
 * @version 2.0
 */
public class ClassificationSchemeValidator {  

	static Assistant myAssistant;
	static String classFile;         //Input file (CSV or YML) containing a user-specified classification scheme.
	static boolean classifyByName;   //Is each category referenced by its identifier in the classification scheme (false) or by the actual name of the category (true)? 
	static String outFormat = "CSV"; //Default format used in printing out the reconstructed classification
	
	public static void main(String[] args) {  

	    System.out.println(Constants.COPYRIGHT);
	    myAssistant = new Assistant();
	   
	    Classification classification;

	    String flag;
	    
	    if (args.length > 0)  {

	    	System.out.println("This utility will validate the specified classification scheme. Once reconstructed, this hierarchy will be printed to standard output.");
	    	
	    	//Specify a configuration file with properties regarding classification hierarchy
	    	classFile = args[0];          //Argument like "./test/OSM_POI_classification.yml"
	    	
	    	//Get the boolean flag that specifies how to reconstruct the classification hierarchy
	    	if (args.length > 1)
	    		flag = args[1];
	    	else {
	    		System.out.println("Which attribute is used in your dataset to refer to items in these classification hierarchy?\n- Specify TRUE if your data references to category names;\n- Specify FALSE (default) if your data references to category identifiers.");
	    		flag = System.console().readLine();    
	    	}
	    		
	    	//Validate boolean parameter
	    	if (flag.equalsIgnoreCase("true") || flag.equalsIgnoreCase("false")) {
	    		classifyByName = Boolean.valueOf(flag);  
	    	} 
	    	else {
	    		classifyByName = false;     //By default, assume that hierarchy is based on category identifiers
	    	}
	
	    	//Check out in which format to print the reconstructed classification scheme
	    	if (args.length > 2) 
	    		outFormat = args[2];             //Only "csv" or "yml" are valid options
	    	if (!(outFormat.equalsIgnoreCase("csv") || outFormat.equalsIgnoreCase("yml"))) {
	    		outFormat = "CSV";     //By default, print the reconstructed hierarchy in CSV format
	    	}
	    	
	        try { 			  
				if (classFile != null)
				{
					classification = new Classification(classFile, classifyByName);    //Parse input file with filters for assigning categories
					
					//Print the reconstructed classification scheme in the specified format
					System.out.println("***************************CLASSIFICATION********************************");
					if (outFormat.equalsIgnoreCase("csv")) {                   //Print reconstructed hierarchy in a CSV-like fashion with TAB delimiter
						System.out.println("UUID" + "\t" + "ID" + "\t" + "CATEGORY" + "\t" + "PARENT_CATEGORY" + "\t" + "EMBEDDED_CATEGORY" + "\t" + "EMBEDDED_SCORE");
			            for (Map.Entry<String, Category> item : classification.categories.entrySet()) {
			            	Category parent = null;	      //Identify the name of the parent category (if exists)        	
			            	if (item.getValue().getParent() != null)
			            		parent = classification.searchById(item.getValue().getParent());
			            	String formatParent = (parent == null) ? "" : ((classifyByName) ? parent.getName() : parent.getId());
			            	String formatCategory = (classifyByName) ? (item.getValue().getId() + "\t" + item.getValue().getName()) : (item.getValue().getName() + "\t" + item.getValue().getId());
			                System.out.println(item.getValue().getUUID() + "\t"  + formatCategory + "\t" + formatParent + "\t" + item.getValue().getEmbedCategory() + "\t" + item.getValue().getEmbedScore());
			            }
					}
					else
					{
						classification.printHierarchyYML();              //Print reconstructed hierarchy in a YML-like fashion
					}
		            System.out.println("**************************************************************************");				
					System.out.println("Number of tiers in this classification hierarchy: " + classification.countTiers());
					System.out.println("Total number of categories identified at all tiers of this classification hierarchy: " + classification.countCategories());
				} 
	      }  
	      // Handle any errors that may have occurred during reading of classification hierarchy  
	      catch (Exception e) {  
	         ExceptionHandler.abort(e, "Failed to read classification hierarchy.");  
	      } 
		  finally {
			  System.out.println(myAssistant.getGMTime() + " Classification hierarchy processed successfully!");
		  }
	    }
	    else {
			System.err.println(Constants.INCORRECT_CLASSIFICATION); 
			System.out.println("USAGE: java -cp target/triplegeo-2.0-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.extra.ClassificationSchemeValidator <path-to-CSV-or-YML-classification-file> <boolean-flag> <output-CSV-or-YML-format>");
			System.exit(1);          //Execution terminated abnormally
	    }
    }  
}  
