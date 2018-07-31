/*
 * @(#) OsmClassification.java	version 1.5   11/7/2018
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
package eu.slipo.athenarc.triplegeo.osm;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * Given a YAML file with OSM tags and their correspondence to categories/subcategories, it creates a representation of a hierarchical classification scheme.
 * LIMITATION: The resulting classification scheme is stored in an intermediate CSV file. 
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 7/9/2017
 * Modified: 7/9/2017; added filters for tags in order to assign categories to extracted OSM features according to a user-specified classification scheme (defined in a YAML file).
 * Last modified by: Kostas Patroumpas, 11/7/2018
 */
public class OSMClassification {

	OSMFilterFileParser filterFileParser;                //Parser for a YAML file containing OSM tags...
	private List<OSMFilter> filters;                     //...specifying filters for assigning categories to OSM features
	private Set<String> tags;                            //OSM tags used in the filters
	String classificationSpec;                           //Path to the classification file
	String outputDir;                                    //Output directory
	String classFile;                                    //Intermediate .CSV file that will contain the classification scheme
	
	/**
	 * Constructor of this class.
	 * @param classSpec  Path to the YAML file containing the classification.
	 * @param outDir   Directory that will hold the intermediate representation of the classification scheme, as will be used during transformation.
	 */
	public OSMClassification(String classSpec, String outDir) {
		classificationSpec = classSpec;
		outputDir = outDir;
		classFile = outputDir + FilenameUtils.getBaseName(classificationSpec) + ".csv";
	}
	
	/**
	 * Applies the user-specified filters over OSM tags and creates an intermediate CSV file with the resulting classification
	 * @return  Path to the intermediate classification file, as will be used during transformation.
	 */
	public String apply() {
	      //Get filter definitions over combinations of OSM tags in order to determine POI categories
	      try {
	    	  System.out.println(Constants.OSMPOISPBF_COPYRIGHT);
		      //Read YML file from configuration settings containing assignment of OSM tags to categories
		      filterFileParser = new OSMFilterFileParser(classificationSpec);    
		      filters = filterFileParser.parse();   //Parse the file containing filters for assigning categories to OSM features
		      if (filters == null)
		    	  throw new FileNotFoundException(classificationSpec);
		      
		      //FIXME: This representation assumes that categories are represented as strings in the YML file for each respective OSM tag	      
		      BufferedWriter csvWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(classFile), StandardCharsets.UTF_8));
	    	  //Write header	
		      csvWriter.write("\"CATEGORY_ID\", \"CATEGORY\", \"SUBCATEGORY_ID\", \"SUBCATEGORY\"");
		      csvWriter.newLine();
  	      
		      //Get all tags
		      tags = filterFileParser.getAllTags();
  	      
		      //Get all categories and prepare a classification based on them
		      Set<String> categories = filterFileParser.getAllCategories();
		      for (String c: categories) 
		      {
		    	  if (c.contains("_"))  //FIXME: Assumption that an "_" character is used as delimiter between items in the various tiers in the category name, e.g., "FOOD,RESTAURANT"
		    	  {
		    		  String[] parts = c.split("_");
		    		  //Prepare a row for an intermediate CSV file that will hold the classification hierarchy  
		    	      csvWriter.write("\"" + parts[0] + "\",\"" + parts[0] + "\",\"" + c + "\",\"" + parts[1] + "\"");  //FIXME: Subcategory identifiers must be identical to those specified in the YML file
		    	      csvWriter.newLine();
		    	  }
		      }
		      csvWriter.close();
		      
	      }
		  catch(Exception e) { 
				ExceptionHandler.abort(e, "Cannot create classification for OSM data. Missing or malformed YML file with classification of OSM tags into categories.");
		  }
	      
	      return classFile;                  //Path to the intermediate classification file
	}
	
	
	/**
	 * Return all OSM tags specifying the filters
	 * @return  A collection of all OSM tags specified in the classification scheme.
	 */
	public Set<String> getTags() {
		return tags;
	}
	
	/**
	 * Return all filters that will be used to assign categories to OSM elements (nodes, ways, relations).
	 * @return  A list with all specified filters, i.e., correspondence of OSM tags to user-specified categories.
	 */
	public List<OSMFilter> getFilters() {
		return filters;
	}
}