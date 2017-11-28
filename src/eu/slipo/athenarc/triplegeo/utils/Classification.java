/*
 * @(#) Classification.java 	 version 1.3   28/11/2017
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

package eu.slipo.athenarc.triplegeo.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.model.dataset.SimpleRMLDataset;
//import be.ugent.mmlab.rml.model.dataset.StdRMLDataset;

/**
 * Parser for a multi-tier classification hierarchy (category/subcategory/...).
 * ASSUMPTION: Each category (at any level) must have a unique name, which is being used as its key the derived dictionary.
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 7/11/2017
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */

public class Classification {
	
	Converter myConverter;
	Assistant myAssistant;
	private String outputFile;                    //Output RDF file
	private Configuration currentConfig;          //User-specified configuration settings
	private static final int MAX_LEVELS = 10;     //Maximum depth in classification hierarchy
	private String splitter = "#";                //Character used to separate category name from its identifier
	private char indent = ' ';                    //Character used in indentation: a PAIR of such same characters is used to mark levels in the hierarchy
	
	//Dictionary for all categories and their parents; their name is used as the key in searching
	public Map<String, Category> categories = new HashMap<String, Category>();  
	
	
	//Constructor
	public Classification(Configuration config, String outFile) {
		
		myAssistant = new Assistant();
		currentConfig = config;
		outputFile = outFile;     //Path to the (YML or CSV) file where classification hierarchy is stored; this path must be included in the configuration settings
		
		//Depending on the type of the file containing hierarchy of categories, call the respective parser
		if (config.classificationSpec.endsWith(".yml"))
			this.parseYMLFile(config.classificationSpec);    //Parse the YML file 
		else if (config.classificationSpec.endsWith(".csv"))
			this.parseCSVFile(config.classificationSpec);    //Parse the CSV file
		else
		{
			System.err.println("ERROR: Valid classification hierarchies must be stored in YML or CSV files.");
			System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
		}
		
		//Once the classification scheme has been constructed, apply transformation to RDF
		//Mode RML: consume records and apply RML mappings in order to get triples
		myConverter = new RMLConverter(currentConfig);
		executeParser4RML();
	}
	
	
	//Parse input YML file with classification hierarchy
	//ASSUMPTION: Each line corresponds to a category; levels are marked with a number of indentation characters at the beginning of each line; no indentation signifies a top-tier category.
	public void parseYMLFile(String classificationFile) {
		
		int numLines = 0;
		Category[] categoryLevels = new Category[MAX_LEVELS + 1];
		
		System.out.println("Starting processing of YML file with classification hierarchy...");
		
		//Parse lines from input YML file with classification hierarchy	
		try {
			//Consume input file line by line and populate the dictionary
			InputStreamReader reader = new FileReader(classificationFile);
			BufferedReader buf = new BufferedReader(reader);
			String line;
			while ((line = buf.readLine()) != null) 
			{
				numLines++;
				
				// Ignore empty lines
				if(line.trim().length() == 0) {
					continue;
				}

				// Find indentation
				// Count leading indentation characters (a pair of them signifies another level down in the hierarchy)
				int level = 0;
				for (char c : line.toCharArray()) {
					if(c != indent) {
						break;
					}
					level++;
				}
				
				level = level/2;   //CAUTION: Two indentation characters signify a level
				
				// Check if for max depth is exceeded
				if (level > MAX_LEVELS) {
					System.err.println("ERROR: Line " +  numLines + ": Maximum depth of classification is " + MAX_LEVELS + " levels.");
					System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
				}
				
				// Get parts
				line = line.trim();
				String[] parts = line.split(splitter, 2);
				if (parts.length != 2) {
					System.err.println("ERROR: Line " +  numLines + ": No '" + splitter + "' character found before identifier.");
					System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
				}
				
				String id = parts[1].replace(splitter.charAt(0),' ').trim();
				String name = parts[0].replace(indent, ' ').trim();
				
//				System.out.print("KEY:" +  id + " CATEGORY: *" + name + "*");
				
				// Add to top tier
				if (level == 0) {
					//Create a new category
					Category category = new Category("", id, name, "");    //No URI; No parent category
					categories.put(name, category);                            //Use name of category as key when searching
					
					// Check for identifier
					if(id.isEmpty()) {
						System.err.println("ERROR: Line " +  numLines + ": No identifier given for top-tier category.");
						System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
					}
					
					// Reset category tiers
					categoryLevels = new Category[MAX_LEVELS + 1];
					categoryLevels[0] = category;
					continue;
				}
				
				// Get parent category
				Category parent = categoryLevels[level - 1];
				if (parent == null) {
					System.err.println("ERROR: Line " +  numLines + ": Invalid indentation.");
					System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
				}
				
//				System.out.println(" PARENT KEY:" + parent.getId() + " -> " + parent.getName());
				
				//Create a new category
				Category category = new Category("", id, name, parent.getId());
				
				// Check if valid
				if (!category.hasId() && !category.hasName()) {
					System.err.println("ERROR: Line " +  numLines + ": Classification must have both a key and a category.");
					System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
				}
				
				// An identifier must be specified after a category name at any level
				if (category.hasName() && !category.hasId()) {
					System.err.println("ERROR: Line " +  numLines + ": No identifier provided for a category.");
					System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
				}
				
				// Add category to the classification
				categories.put(name, category);                    //Use name of category as key when searching
				categoryLevels[level] = category;

			}
			buf.close();   //Close input file
			
			//Check if file is empty
			if ((numLines == 0) || (categories.isEmpty())) {
				System.err.println("ERROR: Classification file is empty.");
				System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
			}
		}
		catch(Exception e) {
			System.err.println("ERROR: Reading classification file failed!");
			System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
		}
		
		System.out.println("Classification hierarchy reconstructed from YML file.");
			
	}
	
	
	//Parse input CSV file with classification hierarchy
	//ASSUMPTION: Each line (record) corresponds to a full path from the top-most to the bottom-most category; at each level, two attributes are given: first the identifier, then the name of the category.
	@SuppressWarnings("resource")
	public void parseCSVFile(String classificationFile) {
		
		int numRecs = 1;                                     //Assuming that the CSV file has a header
		
		System.out.println("Starting processing of CSV file with classification hierarchy...");
		
		//Parse lines from input CSV file with classification records	
		try {
			//Consume input file line by line
			Reader in = new InputStreamReader(new FileInputStream(classificationFile), "UTF-8");
			CSVFormat format = CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withFirstRecordAsHeader();	
			CSVParser dataCSVParser = new CSVParser(in, format);
			
			//Consume each record and populate the dictionary
			for (CSVRecord rec: dataCSVParser.getRecords())
			{
				numRecs++;
				String parent = "";
				int level = 0;
				for (int i=0; i < rec.size(); i+=2)
				{
					if (rec.get(i+1).trim() != "")
					{
						level++;
						String id = rec.get(i).trim();
						String name = rec.get(i+1).trim();
						
						//Create a new category
						Category category = new Category("", id, name, parent);
						
						// Check if valid
						if (!category.hasId() || !category.hasName()) {
//							System.err.println("ERROR: Line " +  numRecs + ": Classification must have both a key and a category.");
							continue;
						}
						
						// An identifier must be specified after a category name at any level
						if (category.hasName() && !category.hasId()) {
							System.err.println("ERROR: Line " +  numRecs + ": No identifier provided for a category.");
							System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
						}
						
						parent = id;        //Current category will be the parent of the one at the next level
						
						//Append this category if it does not already exist in the dictionary
						if (categories.containsKey(name))
							if (categories.get(name).getId().equals(id))
								continue;

						//Otherwise, add category to the classification
						categories.put(name, category);                    //Use name of category as key when searching
/*													
						//Print categories in a YML-like fashion:
						for (int k=1; k < level; k++)
							System.out.print(indent);                      //Indentation character depending on the level
						System.out.println(name + " " + splitter + id);	   //Special character before identifier
*/				
					}
				}		
			}
						
			//Check if file is empty
			if ((numRecs == 0) || (categories.isEmpty())) {
				System.err.println("ERROR: Classification file is empty.");
				System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
			}
		}
		catch(Exception e) {
			System.err.println("ERROR: Reading classification file failed!");
			System.exit(1);                              //Issue signal to the operation system that execution terminated abnormally
		}
		
		System.out.println("Classification hierarchy reconstructed from CSV file.");			
	}

	
	//For a given category name, find the respective the entry in the classification scheme
	public Category search(String key) {
		if (categories.containsKey(key))
        	return categories.get(key);                    //A category may be found at any level in this scheme

		return null;
	}

	
	//For a given category name, identify its respective URI in the classification scheme
	public String getURI(String categoryName) {
		if (categories.containsKey(categoryName))
        	return categories.get(categoryName).getURI();           //A category may be found at any level in this scheme

		return null;
	}

	
	//Return the number of categories in the dictionary
	public int countCategories() {
		return categories.size();
	}
	
	
	//Given a parent identifier, recursively find all its descendants and print them in a YML-like fashion
	private void findDescendants(String parent_id, int level) 
	{
		//Iterate through the dictionary and type all descendants of a given item in the classification hierarchy
		for (Category rs : categories.values()) 
		{
			if ((rs.getParent() != null) && (rs.getParent().equals(parent_id)))
			{
				int lvl = level + 1;
				//Print categories in a YML-like fashion:
				for (int k=1; k < lvl; k++)
					System.out.print(indent);                                      //Indentation character depending on the level
				System.out.println(rs.getName() + " " + splitter + rs.getId());	   //Special character before identifier
				
				//Recursion at the next level
				findDescendants(rs.getId(), lvl);            
			}		
		}
	}
	
	
	//Print the entire classification scheme in a YML-like fashion
	public void printHierarchyYML()
	{
		//Iterate through the dictionary and type all descendants of a given item in the classification hierarchy
		for (Category rs : categories.values()) 
		{
			if (rs.getParent() == null)               //Top-tier categories do not have a parent
			{
				System.out.println(rs.getName() + " " + splitter + rs.getId());	   //Special character before identifier
				findDescendants(rs.getId(), 1);
			}
		}	
	}
	
	  /**
	   * 
	   * Mode: RML -> Parse each item in the classification hierarchy and streamline the resulting triples according to the given RML mapping
	   */
	  private void executeParser4RML() {

	    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	   
	    int numRec = 0;
	    int numTriples = 0;

//	    RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    	  
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			
			//Iterate through all categories
			for (Category rs : categories.values()) 
			{
//				System.out.println("CATEGORY: " + rs.typeContents());
				
		      	//Pass all attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
	      		row.put("CATEGORY_ID", rs.getId());                    //Identifier for this category
	      		row.put("CATEGORY_NAME", rs.getName());                //Name of this category
	      		row.put("CATEGORY_PARENT", rs.getParent());            //Identifier for the parent of this category
	      
		        //Apply the transformation according to the given RML mapping		      
		        myConverter.parseWithRML(row, dataset);
		       
		        myAssistant.notifyProgress(++numRec);
			  
				//Update the URI of this category according to the RML mapping
				rs.setURI(rs.getId());                    //Simple ID for this category
				//rs.setURI(myConverter.getURItemplate4Classification().replace("{" + "CATEGORY_ID" + "}", rs.getId()));   //FIXME: Proper http URIs get distorted during RML processing  
			
			    //Periodically, dump results into output file
				if (numRec % 10 == 0) 
				{
					numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, currentConfig.encoding);
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!	
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