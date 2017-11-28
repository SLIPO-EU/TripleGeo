/*
 * @(#) RMLDatasetConverter.java 	 version 1.3   22/11/2017
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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.openrdf.repository.Repository;

import be.ugent.mmlab.rml.mapdochandler.extraction.std.StdRMLMappingFactory;
import be.ugent.mmlab.rml.mapdochandler.retrieval.RMLDocRetrieval;
import be.ugent.mmlab.rml.model.RMLMapping;
import be.ugent.mmlab.rml.model.TriplesMap;
import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.performer.NodeRMLPerformer;
import be.ugent.mmlab.rml.performer.RMLPerformer;
import be.ugent.mmlab.rml.processor.RMLProcessor;
import be.ugent.mmlab.rml.processor.RMLProcessorFactory;
import be.ugent.mmlab.rml.processor.concrete.ConcreteRMLProcessorFactory;


/**
 * Creates and populates a RML dataset so that data can be serialized into a file.
 * Created by: Kostas Patroumpas, 27/9/2013
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Last modified by: Kostas Patroumpas, 22/11/2017
 */
public class RMLConverter implements Converter {

	private static Configuration currentConfig; 
	
//	RMLDataset dataset;
	RMLPerformer[] performers;
	String[] exeTriplesMap = null;
	Map<String, String> parameters = null;
	TriplesMap[] maps;
	String templateCategoryURI;

	
		  /*
		   * Constructs a RMLConverter object.
		   *
		   * @param config - User-specified configuration for the transformation process.
		   */
		  public RMLConverter(Configuration config) {
			  
		      super();
		    
		      currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DBMS, etc.)  

		      try {
					StdRMLMappingFactory mappingFactory = new StdRMLMappingFactory();
	
					RMLDocRetrieval mapDocRetrieval = new RMLDocRetrieval();
					Repository repository = mapDocRetrieval.getMappingDoc(currentConfig.mappingSpec, org.openrdf.rio.RDFFormat.TURTLE);
					RMLMapping mapping = mappingFactory.extractRMLMapping(repository);
								
					Collection<TriplesMap> triplesMaps;
					triplesMaps = mapping.getTriplesMaps();
					
					maps = triplesMaps.toArray(new TriplesMap[triplesMaps.size()]);
					performers = new RMLPerformer[maps.length];
					
					RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();
					int k = 0;
					for (TriplesMap map : triplesMaps) 
					{	
						//System.out.println(map.getSubjectMap().getStringTemplate());
						
						//Original call to RML processor
						//RMLProcessor subprocessor = factory.create(map.getLogicalSource().getReferenceFormulation(), parameters, map);
						
						//Simplified RML processor without reference to logical source (assuming a custom row as input)
						RMLProcessor subprocessor = factory.create(map);
						
						//Identify the template used for URIs regarding classification
						//FIXME: Assuming that a single attribute "CATEGORY_ID" is always used for constructing the URIs for categories
						if ((map.getSubjectMap().getStringTemplate() != null) && (map.getSubjectMap().getStringTemplate().contains("CATEGORY_ID")))
							templateCategoryURI = map.getSubjectMap().getStringTemplate();
							
						performers[k] = new NodeRMLPerformer(subprocessor);
						k++;
					}
		      } catch(Exception e) { 
		  	    	ExceptionHandler.invoke(e, " An error occurred while creating RML processors with the given triple mappings.");
		  	    }
		      
		  }


		  //Return a handle to the created RDF mode
		  public Model getModel() {
			  
			  return null;
		  }

		  
		  //Used only for streaming results
		  public List<Triple> getTriples() {
			  
			  return null;
			  
		  }

/*		  
		  //Used only when applying RML mapping(s)
		  public RMLDataset getRMLDataset() {
		    	return dataset;
		  }
*/
		  
		/**
		 * 
		 * Handling non-spatial attributes (CURRENTLY supporting 'name' and 'type' attributes only)
		 */
		public void handleNonGeometricAttributes(String resType, String featureKey, String featureName, String featureClass) throws UnsupportedEncodingException, FileNotFoundException {}

		
		/**
		 * 
		 * Convert representation of a geometry into suitable triples
		 */
		public void parseGeom2RDF(String t, String resource, String geom, int srid) {}


		/**
		 * 
		 * Convert a row (with several attribute values, including geometry) into suitable triples according to the specified mappings
		 */
		public void parseWithRML(HashMap<String, String> row, RMLDataset dataset)
		{
			  //Parse the given record with each of the performers specified in the RML mappings
		      for (int j=0; j<performers.length; j++)
		      {
		    	  performers[j].perform(row, dataset, maps[j], exeTriplesMap, parameters, false);
		      }
			
		}
	
		/**
		 * 
		 * Return the URI template used for all subjects in RDF triples concerning the classification hierarchy
		 */
		public String getURItemplate4Classification()
		{
			return templateCategoryURI;
			
		}
		  /**
		   * 
		   * Serialize the given RML dataset as triples written into a file.
		   */
		  public int writeTriples(RMLDataset dataset, BufferedWriter writer, org.openrdf.rio.RDFFormat rdfFormat, String encoding)
		  {
			  	int numTriples = 0;
			  	try {
				    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
					dataset.dumpRDF(byteStream, rdfFormat);
//					String outputString = byteStream.toString(encoding);
					
					String outputString = byteStream.toString("ISO-8859-1");
			
					writer.write(outputString);
					numTriples = dataset.getSize();
			
					dataset.closeRepository();
			  	}
		  	    catch(Exception e) { 
		  	    	ExceptionHandler.invoke(e, " An error occurred when dumping RDF triples into the output file.");
		  	    }
			  			  
				return numTriples;
		  }
		  
		
		/**
		 * 
		 * Initialize dataset where resulting triples will be collected
		 */
//		public void initDataset()
//		{
//			dataset = new StdRMLDataset();
//		}

}
