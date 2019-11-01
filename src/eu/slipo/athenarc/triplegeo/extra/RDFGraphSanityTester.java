/*
 * @(#) RDFGraphSanityTester.java	version 2.0  23/10/2019
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
package eu.slipo.athenarc.triplegeo.extra;

import java.io.FileInputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
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

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Constants;

/**
 * Loads triples from data file(s) into a disk-based RDF graph and runs a simple sanity test with a user-specified SELECT query in SPARQL.
 * This auxiliary utility can be used to verify that the transformed triples are valid and queryable. 
 * USAGE: Execution command over JVM:
 *           java -cp target/triplegeo-2.0-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.extra.RDFGraphSanityTester <path-to-triples-file(s)> <triple-serialization-format> <path-to-temporary-dir> <path-to-SPARQL-query-file>
 * ARGUMENTS: (1) Path to the RDF file(s) that constitute the graph; all of them must have the same serialization format.
 *            (2) Serialization of the RDF file(s), e.g., N-TRIPLES or TTL.
 *            (3) Path to an existing directory on disk where the RDF graph model will be temporarily created.
 *            (4) Path to the file with the SPARQL SELECT command that will be used to query the RDF graph and extract results.
 *            
 * @author Kostas Patroumpas
 * @version 2.0 
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/9/2017
 * Last modified by: Kostas Patroumpas, 23/10/2019
 */
public class RDFGraphSanityTester {

	  public static void main(String[] args) {  

		  System.out.println(Constants.COPYRIGHT);

		  if (args.length >= 4)  
		  {
			  Assistant myAssistant = new Assistant();
			  Dataset dataset = null;
			  Model model = null;
			  String dir;
			  
			  List<String> attrList;              //Attribute names in the records returned by the SPARQL query
			
			  //ARG #1: Path to the RDF file(s) that constitute the graph; all of them must have the same serialization format
			  //Specification of multiple RDF files may be separated by ;
			  String inputFileNames = args[0];           //Example: ./test/output/sample1.nt;./test/output/sample2.nt			  
			  String[] inputFiles = inputFileNames.split(";");     //MULTIPLE input file names separated by ;
			  
			  //ARG #2: Serialization of the RDF file(s)
			  String rdfSerialization = args[1];  //Example: N-TRIPLES
			  
			  //ARG #3: Path to an existing directory where the RDF graph model will be created on disk
			  String tmpDir = args[2];            //Example: ./tmp          
			  
			  //ARG #4: Path to the file with the SPARQL SELECT command that will be used to query the RDF graph and extract results
			  String sparqlFile =  args[3];       //Example: ./test/conf/sanity.sparql
			  
			  try { 	
				    System.out.println("Loading RDF triples to RDF graph...");
				    
				    //Create a temporary directory to hold intermediate data for this graph
				    dir = myAssistant.createDirectory(tmpDir);
			      
					//Directory to be used for disk storage of the RDF model to be created
				    dataset = TDBFactory.createDataset(dir) ;
				          
				    //The RDF graph model to be used in the disk-based transformation process
				    model = dataset.getDefaultModel() ;
				    
				    //Ingest RDF triples from each file into this graph according to the given serialization
				    for (String rdfFile: inputFiles)
				    {
				    	System.out.print("Loading file " + rdfFile + "...");
				    	RDFDataMgr.read(model, rdfFile, myAssistant.getRDFLang(rdfSerialization));
				    	System.out.println(" Completed!");
				    }
				    System.out.println("The resulting RDF graph contains " + model.getGraph().size() + " statements in total.");
				    
					//Read a valid SPARQL query from a file
					//CAUTION! This must be a valid SELECT query that reflects the schema and properties of the RDF graph
					FileInputStream inputStream = new FileInputStream(sparqlFile); 
					String q = IOUtils.toString(inputStream, "UTF-8");
					 
					Query qry = QueryFactory.create(q);
				    QueryExecution qe = QueryExecutionFactory.create(qry, model);
				    ResultSet rs = qe.execSelect();
				    int n = 0;
				    
				    System.out.println("************QUERY RESULTS (NULL values are omitted)*************");
				    
				    //Get list of attribute names in the SELECT query
				    attrList = rs.getResultVars();
				    System.out.println(attrList.toString());
				    
				    //Iterate through query results; each result concerns an entity with a distinct URI
				    while (rs.hasNext())
				    {
				        QuerySolution sol = rs.nextSolution();  
	
				        //Collect values for all attributes specified in the SPARQL query
				        for (int i=0; i < attrList.size(); i++)
				        {
				        	RDFNode r = sol.get(attrList.get(i));
				        	String s;
			        	
				        	if (r != null) {
				        		if (r.isLiteral())
				        			s = sol.getLiteral(attrList.get(i)).getString();    //String or numeric Literal
				        		else
				        			s = r.toString();                                   //URI
				        	
				        		//Print results to the standard output	
				        		System.out.print(s + " ");    		
				        	}
				        }
				        System.out.println();
	
				        n++;        
				    }
	
				    qe.close();
				    
				    System.out.println(n + " results retrieved from the RDF graph with the submitted query.");
				    System.out.println("Sanity test completed successfully.");
		      }  
		      // Handle any errors that may have occurred
		      catch (Exception e) {  
		         e.printStackTrace();  
		         System.out.println("Sanity test ended abnormally. Please check the input file(s) for errors in triple statements or in SPARQL query specifications.");
		      } 
			  finally {
				  //Close the graph model
				  model.close();
				  TDBFactory.release(dataset);
			  }
		  }
		    else {
				System.err.println("Incorrect number of arguments. \nUsage: java -cp <PATH-to-TripleGeo-JAR> eu.slipo.athenarc.triplegeo.SanityTester <path-to-triples-file(s)> <triple-serialization-format> <path-to-temporary-dir> <path-to-SPARQL-query-file>"); 
				System.exit(1);          //Execution terminated abnormally
		    }
	   }  
	   
}