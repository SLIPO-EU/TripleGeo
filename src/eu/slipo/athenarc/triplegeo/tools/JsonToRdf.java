/*
 * @(#) JsonToRdf.java 	 version 1.7   25/10/2018
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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from JSON documents.
 * LIMITATIONS: The entire JSON document is read in memory before parsing, so large files require suitable configuration of the JVM heap size. 
 * @author Kostas Patroumpas
 * @version 1.7
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 19/7/2018
 * Modified: 20/7/2018, added support for exporting all available non-spatial attributes as properties
 * Modified: 25/10/2018; integrate handling of a user-specified classification scheme for features.
 * Last modified by: Kostas Patroumpas, 25/10/2018
*/

public class JsonToRdf {  

	Converter myConverter;
	Assistant myAssistant;
	private MathTransform reproject = null;
	int sourceSRID;                        //Source CRS according to EPSG 
	int targetSRID;                        //Target CRS according to EPSG
	private Configuration currentConfig;   //User-specified configuration settings
	private Classification classification; //Classification hierarchy for assigning categories to features
	private String inputFile;              //Input JSON file
	private String outputFile;             //Output RDF file

	//Initialize a CRS factory for possible reprojections
	private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	/**
	 * Constructor for the transformation process from JSON document to RDF.
	 * @param config  Parameters to configure the transformation.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param inFile  Path to input JSON file.
	 * @param outFile  Path to the output file that collects RDF triples.
	 * @param sourceSRID  Spatial reference system (EPSG code) of the input JSON file.
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @throws ClassNotFoundException
	 */
	public JsonToRdf(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  currentConfig = config;
		  classification = classific;
		  inputFile = inFile;
		  outputFile = outFile;
		  this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      myAssistant = new Assistant();
	      
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.targetCRS != null) {
		  	    try {
		  	        boolean lenient = true; // allow for some error due to different datums
		  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
		  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
		  	        reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);  
		  	        
		  	        //Needed for parsing original geometry in WTK representation
		  	        GeometryFactory geomFactory = new GeometryFactory(new PrecisionModel(), sourceSRID);
		  	        myAssistant.wktReader = new WKTReader(geomFactory);
		  	        
		  		} catch (Exception e) {
		  			ExceptionHandler.abort(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
		  		}
	      }
	      else  //No transformation specified; determine the CRS of geometries
	      {
	    	  if (sourceSRID == 0)
	    		  this.targetSRID = 4326;          //All features assumed in WGS84 lon/lat coordinates
	    	  else
	    		  this.targetSRID = sourceSRID;    //Retain original CRS
	      }
	      
	      // Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }
	      
	  }

 
	/**
	 * Applies transformation according to the configuration settings.
	 */
	public void apply() {
		
	      try {
	    	    //Open the JSON file for parsing
				byte[] json = Files.readAllBytes(Paths.get(inputFile));

				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig, outputFile);
				  			
				  //Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
				  parseDocument(json);
				  
				  //Export the RDF graph into a user-specified serialization
				  myConverter.store(myAssistant, outputFile);
				  
				  //Remove all temporary files as soon as processing is finished
				  myAssistant.removeDirectory(myConverter.getTDBDir());
				}
				else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig, outputFile);
				
				  //Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
				  parseDocument(json);
				  
				  //Finalize the output RDF file
				  myConverter.store(myAssistant, outputFile);
				}
				else    //TODO: Implement method for handling transformation using RML mappings
				{
					System.err.println("Transformation of JSON data is possible under either GRAPH or STREAM mode. RML mode is currently not supported.");
					throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
				}
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "");
	  	  }

	}

	
	/**
	 * Parses and possibly flattens a JSON object node.
	 * @param jsonNode  The JSON node to be parsed.
	 * @return  A map with (flattened) properties as keys and their respective values as identified in the given JSON node.
	 */
	private Map<String, String> parseObjectNode(JsonNode jsonNode) {
		
		//Internal representation of a distinct item (feature with spatial and non-spatial attributes)
	    Map<String, String> record = new HashMap<>();
	    	
	    //Flatten nested properties
	    Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
	    iterator.forEachRemaining(node -> flattener(record, node, new ArrayList<String>()));
	    
	    return record;
	}

	/**
	 * Parses and possibly flattens a JSON properties node (i.e., properties listed as distinct items in an array).
	 * @param record  A map updated with (flattened) properties as keys and their respective values as identified in the given JSON node.
	 * @param key  The name of the parent property.
	 * @param k  The serial number of the item in the underlying array.
	 * @param jsonNode  The JSON node to be parsed.
	 */
	private void parseNestedArrayNode(Map<String, String> record, String key, int k, JsonNode jsonNode) {
			
	    //This list holds the parent property and the serial number of the array item.
	    //These will be used as suffixes in the resulting flattened keys.
	    List<String> names = new ArrayList<String>();
	    names.add(key);
	    names.add(Integer.toString(k));
	    
	    //Flatten nested properties
	    Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
	    iterator.forEachRemaining(node -> flattener(record, node, new ArrayList<String>(names)));   //Include suffixes in the names of the flattened keys
	}
	
	/**
	 * Handles a nested array of properties under an identified object (feature), by essentially parsing each of them.
	 * @param key  The name of the parent property.
	 * @param arrayNode  A JSON node representing an array of children nodes.
	 */
	private void parseNestedArray(Map<String, String> record, String key, ArrayNode arrayNode) {
		
		int k = 0;           //Enumerate items in the nested array
		//Parse each constituent object node in the nested array
	    for (JsonNode jsonNode : arrayNode) {  	
	    	parseNestedArrayNode(record, key, k, jsonNode);            //Prefix all resulting properties with the property key and the serial number in the array
	    	k++;
	    }
	}

	
	/**
	 * Parses an array of object nodes, by invoking processing for each of them.
	 * @param arrayNode  The parent node representing an array of object nodes.
	 */
	private void parseArrayNode(ArrayNode arrayNode) {
		
	    for (JsonNode jsonNode : arrayNode) {
	    	processRecord(parseObjectNode(jsonNode));
	    }
	}

	/**
	 * Flattens any nested properties in the given JSON node.
	 * @param record  Map structure to collect the flattened properties (keys) and their values.
	 * @param node  JSON node to be flattened.
	 * @param names  Prefix names for the keys to be generated.
	 */
	private void flattener(Map<String, String> record, Map.Entry<String, JsonNode> node, List<String> names) {
		
	    names.add(node.getKey());            //The key of this node will be used as prefix for any children nodes.
	    	
	    //Handle possible cases of JSON nodes
	    if (node.getValue().isArray()) {
	    	parseNestedArray(record, node.getKey(), (ArrayNode) node.getValue());   //Process array of nested children nodes, each of them representing properties of a feature
	    } 
//	    else if (node.getValue().isObject()) {
//	    	processObjectNode(node.getValue());
//	    }
	    else if (node.getValue().isNull()) {                      //Property without a value  
	    	//Combine prefixes collected in the hierarchy to construct the name of this property using a "." between successive components
	        String name = names.stream().collect(Collectors.joining("."));    
	        record.put(name, null);
	    } 
	    else if (node.getValue().isContainerNode()) {            //This node contains other objects or arrays, ...
	        node.getValue().fields()                             //... so it should be flattened
	            .forEachRemaining(nested -> flattener(record, nested, new ArrayList<>(names)));
	    } 
	    else {                                                   //This is a object property
	    	//Combine prefixes collected in the hierarchy to construct the name of this property using a "." between successive components
	        String name = names.stream().collect(Collectors.joining("."));
	        record.put(name, node.getValue().asText());          //Handle all values as strings
	    }
	}


	/**
	 * Submit a single feature (with a geometry and all its non-spatial properties) for transformation.
	 * @param record  Map structure that has collected all flattened properties (as keys) and their respective values.
	 */
	private void processRecord(Map<String, String> record) {
   
    	//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
		//String uuid = myAssistant.getUUID(currentConfig.featureSource + record.get(currentConfig.attrKey)).toString();
		
		String wkt = null;
		
	    //CAUTION! Construct WKT from longitude/latitude values, so only point features can be constructed
	    if ((record.get(currentConfig.attrX) != null) && (record.get(currentConfig.attrY) != null)) {
		    wkt = "POINT (" + record.get(currentConfig.attrX) + " " + record.get(currentConfig.attrY) + ")";
		    
		    //CRS transformation
		    if ((wkt != null) && (reproject != null))
		    	wkt = myAssistant.wktTransform(wkt, reproject);     //Get transformed WKT representation
	    }
	    
	    //Process all available attributes (including geometry)
		//CAUTION! Currently, each non-spatial attribute name is used as the property in the resulting triple
		if (!record.isEmpty())
			myConverter.parse(myAssistant, wkt, record, classification, targetSRID, "POINT");
	}

	
	/**
	 * Parse each item in the input JSON document in order to create the necessary triples (including geometric and non-spatial attributes).  
	 * @param json   The JSON document having (maybe nested) properties per item.
	 */
	private void parseDocument(byte[] json) {

	      try {
	    	  //Deserialize JSON input data
	    	  ObjectMapper mapper = new ObjectMapper();
	    	  JsonNode matrix = mapper.readValue(json, JsonNode.class);  
	    	  //Iterate over each node identified in the input
	    	  for (JsonNode arrayItem : matrix) {
	    		  //JSON document may contain multiple arrays of items
	    		  if (arrayItem.isArray())
	    			  parseArrayNode((ArrayNode) arrayItem);        //Deserialize and then process each constituent item
	    		  else
	    			  processRecord(parseObjectNode(arrayItem));    //This is a distinct item, so extract it in a suitable feature representation      	  
	    	  }   	  	    	  
	      }     
	      catch (Exception e) {     // Handle any errors that may have occurred
	         e.printStackTrace();  
	      }  

	}

}  
