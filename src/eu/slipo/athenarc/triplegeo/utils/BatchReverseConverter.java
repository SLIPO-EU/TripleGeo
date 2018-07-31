/*
 * @(#) BatchReverseConverter.java	version 1.5   28/2/2018
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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;

import eu.slipo.athenarc.triplegeo.utils.ReverseConverter;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.ReverseConfiguration;


/**
 * Recreates a disk-based RDF graph and extracts its contents through a user-specified SELECT query in SPARQL. 
 * Results can be written to a typical geographical files (currently supported: CSV and ESRI Shapefiles). 
 * Attribute names in the SELECT query will become the column names in the resulting files. 
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/9/2017
 * Modified: 10/1/2018; exporting resulting records in batches
 * Last modified: 28/2/2018
 */
public class BatchReverseConverter {  

	Assistant myAssistant;
	String[] inputFiles;                             //Paths to multiple input files (of exactly the serialization and ontology) that will be read into the RDF graph
	Dataset dataset;                                 //RDF dataset to represent an unnamed, default RDF graph that will be queried
	String dir; 	                                 //Path to the local directory to hold the disk-based RDF graph for this transformation process 
	
	Model model;                                     //Jena model that contains all RDF triples
	List<List<String>> results;                      //Collection of resulting records
	List<String> attrList;                           //Attribute names in the resulting records
	
	private ReverseConfiguration currentConfig;      //Reverse configuration parameters
	

	/**
	 * Constructs a ReverseConverter object.
	 * @param config   User-specified configuration for the reverse transformation process.
	 * @param inFiles  Array of input file names
	 */
	public BatchReverseConverter(ReverseConfiguration config, String[] inFiles) {
		    
		    currentConfig = config;       //Configuration parameters as set up by the various reverse conversion utilities (CSV, SHP) 
		    inputFiles = inFiles;         //Array of input file names
		    
		    myAssistant = new Assistant();    
	}
		
	  
	/**
	 * Creates a graph that can store RDF triples and enable SPARQL queries.
	 */
	public void createModel() {
		
		  System.out.print(myAssistant.getGMTime() + " Initializing RDF graph to hold input triples...");  
		
		  //Create a temporary directory to hold intermediate data for this graph
		  dir = myAssistant.createDirectory(currentConfig.tmpDir);
	      
		  //Directory to be used for disk storage of the RDF model to be created
	      dataset = TDBFactory.createDataset(dir) ;
	          
	      //The graph model to be used in the disk-based transformation process
	      model = dataset.getDefaultModel() ;
	      
	      System.out.println(" Done!");
	      
	      //ALTERNATIVE (NOT-USED): This graph model is memory-based and can handle smaller RDF files
//		  Model model = ModelFactory.createDefaultModel() ;
			
	      //From each output file, read triples into the model according to the given serialization
		  for (final String inFile: inputFiles) {
			//Identify the path to the RDF file
			System.out.print(myAssistant.getGMTime() + " Reading triples from file " + inFile + "...");
			RDFDataMgr.read(model, inFile, myAssistant.getRDFLang(currentConfig.serialization));	
//			TDBLoader.load( ((DatasetGraphTransaction)dataset.asDatasetGraph()).getBaseDatasetGraph() , outFile, true);
			System.out.println(" Done!");
		  }
		  
		  System.out.println(myAssistant.getGMTime() + " RDF graph loaded successfully and contains " + model.getGraph().size() + " statements in total.");		  
	}
	
	
	/**
	 * Closes the graph model and releases all system resources.
	 */
	public void closeModel() {
		  //Close the graph model
		  model.close();
		  TDBFactory.release(dataset);
	}
	
	
	/**
	 * Submits a SPARQL query and evaluates it against this model.
	 * @param sparql  A SELECT query in SPARQL against the RDF graph. Syntax must comply will the ontology of the model.
	 * @param conv  A ReverseConverter object that will apply the reverse transformation against the results of the query.
	 */
	public void queryModel(String sparql, ReverseConverter conv) {
		
		Query qry = QueryFactory.create(sparql);
	    QueryExecution qe = QueryExecutionFactory.create(qry, model);
	    ResultSet rs = qe.execSelect();
	    int n = 0;
	    int numRecs = 0;
	    
	    //Simple container of all resulting records (each one represented as a list of string values)
	    results =  new ArrayList<List<String>>();
	    
	    //Get list of attribute names in the SELECT query
	    attrList = rs.getResultVars();
	    
	    boolean schemaFixed = false;
	    
	    //Iterate through query results
	    while (rs.hasNext())
	    {
	        QuerySolution sol = rs.nextSolution();  
	        List<String> valueList = new ArrayList<String>(attrList.size());        //List of values in this result record
	        
	        //Determine schema, i.e., attribute names and their respective data types from the values returned in the first record
	        if (!schemaFixed)
	        	schemaFixed = conv.createSchema(attrList, sol);
	        
	        //Collect not null values for all attributes specified in the SPARQL query
	        for (int i=0; i < attrList.size(); i++)
	        {
	        	String val = "";
	        	RDFNode r = sol.get(attrList.get(i));

	        	//Inspect attribute values and store them into the resulting record
	        	if (r != null)
	        	{
	        		if (r.isLiteral())
		        		val = sol.getLiteral(attrList.get(i)).getString();
		        	else
		        		val = r.toString();
	        		valueList.add(i, val);
	        	}
	        	else
	        		valueList.add(i, "");     //Substituting NULL values with a blank string     		
	        }
	        
	        //Add attribute values for this record to the result collection
	        results.add(valueList);
	        n++;
	        
	        //Periodically, store records into the output file in order to avoid exhaustion of memory resources, ...
	        if (n % currentConfig.batch_size == 0)
	        {
	        	numRecs = conv.store(attrList, results);
	        	//... and clean container in order to hold a fresh batch of results
	        	results.clear();
	        	System.out.print(myAssistant.getGMTime() + " Processed " + n + " records..." + "\r");
	        }    
	    }

	    //Finally, store the last (incomplete) batch of results and purge them from the container
	    numRecs = conv.store(attrList, results);
    	results.clear();
	    qe.close();
	    
	    System.out.println(myAssistant.getGMTime() + " " + n + " results retrieved from the RDF graph. " + numRecs + " features created.");
	}
	
}  
