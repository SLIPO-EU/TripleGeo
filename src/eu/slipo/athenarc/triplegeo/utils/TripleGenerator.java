/*
 * @(#) TripleGenerator.java 	 version 1.4   2/3/2018
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;

import eu.slipo.athenarc.triplegeo.utils.Mapping.mapProperties;

/**
 * Generates a collection of RDF triples from the attributes of a given feature.
 * @author Kostas Patroumpas
 * @version 1.4
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 19/12/2017
 * Modified: 21/12/2017, added support for object and data properties according to the SLIPO ontology
 * Modified: 23/12/2017, added support for reading attribute mappings from YML file
 * Modified: 1/2/2018, added export of classification scheme for SLIPO
 * Modified: 7/2/2018, added export of all thematic attributes as triples with their original attribute name as RDF property
 * Modified: 14/2/2018; collecting attribute statistics on-the-fly while transforming each feature
 * Modified: 15/2/2018; added support for transforming thematic attributes according to a custom ontology specified in .YML
 * Modified: 21/2/2018; added support for calculated area and perimeter for geometries
 * TODO: Calculate area and perimeter of geometries in standard units (e.g., SI meters and square meters)
 * TODO: Values in thematic (non-spatial) attributes should be cleaned from special characters (e.g., newline, quotes, etc.) that may be problematic in the resulting triples
 * Last modified: 2/3/2018
 */

public class TripleGenerator {

	private static Configuration currentConfig;
	
	private List<Triple> results;          //Container of resulting triples

	Mapping attrMappings = null;           //Mapping of thematic attributes (input) to RDF predicates (output)
	Map<String, String> prefixes;          //Prefixes for namespaces employed during transformation and serialization of RDF triples
	String attrCategoryURI = null;         //Attribute used for the URI of categories, as specified in the mapping of thematic attributes
	String attrDataSource = null;          //Attribute used for the name of data source, as specified in the mapping of thematic attributes
	
	Map<String, Integer> attrStatistics;   //Statistics for each attribute
	
    /**
     * Constructs a TripleGenerator for transforming a feature (as a record of attributes) into RDF triples
     * @param config  User-specified configuration for the transformation process.
     */
	public TripleGenerator(Configuration config) {
		    
	    currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DBMS, etc.) 
	    
	    results = new ArrayList<>();          //Hold a collection of RDF triples resulting from transformation
  
	    attrStatistics = new HashMap<String, Integer>();
	    
	    //Keep prefixes as specified in the configuration
	    prefixes = new HashMap<String, String>();
	    for (int i=0; i<currentConfig.prefixes.length; i++)
	    	prefixes.put(currentConfig.prefixes[i].trim(), currentConfig.namespaces[i].trim());
	    
	    //Attribute mappings should have been properly configured in a .YML file
	    if (currentConfig.mappingSpec != null)
	    {
		    attrMappings = new Mapping();

		    //Read mapping file from the path specified in configuration settings
		    attrMappings.createFromFile(currentConfig.mappingSpec); 
			
		    //Identify the extra attributes for category URIs and name of data source as specified in the mapping file
		    for (String key: attrMappings.getKeys())
		    {
		    	if (attrMappings.find(key).entityType.contains("category"))
		    		attrCategoryURI = key;
		    	if (attrMappings.find(key).predicate.contains("sourceRef"))
		    		attrDataSource = key;
		    }
	    }
	    //Otherwise, give default names to these extra attributes
	    if (attrCategoryURI == null)
	    	attrCategoryURI = "CATEGORY_URI";
	    if (attrDataSource == null)
	    	attrDataSource = "DATA_SOURCE";
	 }


	 /**
	  * Provides a collection of triples resulting from conversion (usually from a single input feature)
	  * @return  A list of RDF triples
	  */
	  public List<Triple> getTriples() {
		  
		  return results;	  
	  }

	  
	  /**
	   * Cleans up all triples resulted from conversion so far
	   */
	  public void clearTriples() {
		  
		  results.clear();	  
	  }
	    

	  /**
	   * Update statistics (currently only a counter) of values transformed for a particular attribute
	   * @param attrKey  The name of the attribute
	   */
	  private void updateStatistics(String attrKey) {
		  
			if ((attrStatistics.get(attrKey)) == null)
  				attrStatistics.put(attrKey, 1);                                  //First occurrence of this attribute
  			else
  				attrStatistics.replace(attrKey, attrStatistics.get(attrKey)+1);  //Update count of NOT NULL values for this attribute
	  }
	  

	  /**
	   * Provides the statistics collected during transformation (i.e., count of transformed values per attribute)
	   * @return  A Map containing attribute names (keys) and their respective counts (values)
	   */
	  public Map<String, Integer> getStatistics() {

		  return attrStatistics;
	  }
	  

	  /**
	   * Converts the given feature (a tuple of thematic attributes and its geometry WKT) into RDF triples
	   * @param uuid  The UUID to be assigned to the URIs in the resulting triples
	   * @param row  Attribute values for each thematic (non-spatial) attribute
	   * @param wkt  Well-Known Text representation of the geometry  
	   * @param targetSRID  The EPSG identifier of the Coordinate Reference System of the geometry
	   * @param classific  The classification scheme used in the category assigned to the feature
	   * @return  The URI assigned to this feature and used in its resulting RDF triples
	   */
	  public String transform(String uuid, Map<String,String> row, String wkt, int targetSRID, Classification classific) {

		String uri = null;
		try {
  	    	//Create a URI identifier for the RDF resource
  	        String encodingResource = URLEncoder.encode(uuid, Constants.UTF_8).replace(Constants.WHITESPACE, Constants.REPLACEMENT);	  	      
  	        uri = currentConfig.featureNS + encodingResource;
  	        
	        //Parse geometric representation (including encoding to the target CRS)
	        if (wkt != null)
	        {
	        	transformGeometry2RDF(uri, wkt, targetSRID);
/*			        	
	        	//*******************************************
	        	//Only used for issuing extra lon/lat triples according to WGS84 GeoPosition RDF Vocabulary			        	
	        	coords = Assistant.getLonLatCoords(wkt, targetSRID);
	        	if (coords != null)
	        	{
	        		row.put("longitude", "" + coords[0]);
	        		row.put("latitude", "" + coords[1]);
	        	}			        	
	        	//*******************************************
*/			        	
	        }
        
	        if (attrMappings != null) 
	        	//Handling of non-spatial attributes based on user-specified mappings to a custom ontology
	        	transformCustomThematic2RDF(uri, row, classific);
	        else
				//Otherwise, each attribute name is used as the property in the resulting triple with values as literals
		        transformPlainThematic2RDF(uri, row); 	        
		}
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
			
		return uri;                 //Return the URI assigned to this feature
	  }
	  

   /**
    * Handles all thematic (i.e., non-spatial) attributes of a feature, by simply issuing a triple with the original attribute name as property
    * @param uri  The URI assigned to this feature
    * @param attrValues  Attribute values for each thematic (non-spatial) attribute of the feature
    * @throws UnsupportedEncodingException
    */
	public void transformPlainThematic2RDF(String uri, Map<String, String> attrValues) throws UnsupportedEncodingException {   
		
  	    try 
  	    {
	      	//Also include information about the data source provider as specified in the configuration
	      	attrValues.put(attrDataSource, currentConfig.featureSource);
	      	
  	        //Insert literals for each attribute
  	        for (String key: attrValues.keySet())
  	        {
  	        	if (!key.equals(currentConfig.attrGeometry))    	  //With the exception of geometry, create one triple for each attribute value
  	        	{
  	        		String val = attrValues.get(key);
  	        		if ((val != null) && (!val.equals("")) && (!val.contains("Null")))       //Issue triples for NOT NULL/non-empty values only
  	        		{
  	        			createTriple4PlainLiteral(uri, currentConfig.ontologyNS + key, val);
  	        			updateStatistics(key);                        //Update count of NOT NULL values transformed for this attribute
  	        		}
  	        	}
  	        }
  	    }
  	    catch(Exception e) { 
  	    	ExceptionHandler.warn(e, " An error occurred when attempting transformation of a thematic attribute value.");
  	    }
  	    
    }
	
	
	/**
	 * Converts representation of a geometry WKT into suitable RDF triple(s) depending on the specified spatial ontology	
	 * @param uri  The URI assigned to this feature
	 * @param wkt  Well-Known Text representation of the geometry 
	 * @param srid  The EPSG identifier of the Coordinate Reference System of the geometry
	 */
	public void transformGeometry2RDF(String uri, String wkt, int srid) {	 
		
      try {

        //Distinguish geometric representation according to the target store (e.g., Virtuoso, GeoSPARQL compliant etc.)
        if (currentConfig.targetGeoOntology.equalsIgnoreCase("wgs84_pos"))        //WGS84 Geoposition RDF vocabulary
        	insertWGS84Point(uri, wkt);
        else if (currentConfig.targetGeoOntology.equalsIgnoreCase("Virtuoso"))    //Legacy Virtuoso RDF point geometries
        	insertVirtuosoPoint(uri, wkt);
        else
        	insertWKTGeometry(uri, wkt, srid);            //Encoding geometry with a specific CRS is allowed in GeoSPARQL only
        
        //Type according to GeoSPARQL feature
        createTriple4Resource(uri, RDF.type.getURI(), currentConfig.geometryNS + Constants.FEATURE);
        
      } catch (Exception e) {
    	  ExceptionHandler.warn(e, " An error occurred during transformation of a geometry.");
      }
	    
	}


	/**
	 * Inserts a typical WKT geometry of a spatial feature into the Jena model (suitable for GeoSPARQL compliant stores)
	 * @param uri  The URI assigned to this feature
	 * @param wkt  Well-Known Text representation of the geometry 
	 * @param srid  The EPSG identifier of the Coordinate Reference System of the geometry
	 */
	private void insertWKTGeometry(String uri, String wkt, int srid) {	
		
	  	  //Detect geometry type from the WKT representation (i.e., getting the text before parentheses)
	  	  String geomType = " ";
	  	  int a = wkt.indexOf("(");
	  	  if (a > 0)
	  		  geomType = wkt.substring(0, a).trim();
	  	
	  	  //Create a link between a spatial feature and its respective geometry
	  	  createTriple4Resource(uri, Constants.NS_GEO + "hasGeometry", uri + Constants.FEAT);
  	
	  	  //Insert a triple for the geometry type (e.g., point, polygon, etc.) of a feature
	  	  createTriple4Resource(uri + Constants.FEAT, RDF.type.getURI(), Constants.NS_SF + geomType);
/*
	  	  //Insert extra properties concerning the CALCULATED area OR perimeter
	  	  if (geomType.toUpperCase().contains("POLYGON"))
	  	  {
		  	  insertTriple4TypedLiteral(uri, currentConfig.ontologyNS + "area", "" + Assistant.getArea(wkt), TypeMapper.getInstance().getSafeTypeByName(Constants.NS_XSD + "float"));
		  	  insertTriple4TypedLiteral(uri, currentConfig.ontologyNS + "length", "" + Assistant.getLength(wkt), TypeMapper.getInstance().getSafeTypeByName(Constants.NS_XSD + "float"));
	  	  }
	  	  //Insert an extra property concerning the CALCULATED length of this linestring
	  	  else if (geomType.toUpperCase().contains("LINE"))
		  	  insertTriple4TypedLiteral(uri, currentConfig.ontologyNS + "length", "" + Assistant.getLength(wkt), TypeMapper.getInstance().getSafeTypeByName(Constants.NS_XSD + "float"));
*/
	  	  //Encode SRID information before the WKT literal
	  	  wkt = "<http://www.opengis.net/def/crs/EPSG/0/" + srid + "> " + wkt;

	  	  //Triple with the WKT literal
	  	  createTriple4TypedLiteral(uri + Constants.FEAT, Constants.NS_GEO + Constants.WKT, wkt, TypeMapper.getInstance().getSafeTypeByName(Constants.NS_GEO + Constants.WKTLiteral));

	}


	/**
	 * Insert a Point geometry of a spatial feature into the Jena model according to legacy Virtuoso RDF geometry specifications (concerning point geometries only)
	 * @param uri  The URI assigned to this feature
	 * @param pointWKT  Well-Known Text representation of the (point) geometry 
	 */
	private void insertVirtuosoPoint(String uri, String pointWKT) {  

		createTriple4TypedLiteral(uri, Constants.NS_POS + Constants.GEOMETRY, pointWKT, TypeMapper.getInstance().getSafeTypeByName(Constants.NS_VIRT + Constants.GEOMETRY));
	    
	}
		  

	/**
	 * Insert a Point geometry of a spatial feature into the Jena model according to legacy WGS84 Geoposition RDF vocabulary
	 * @param uri  The URI assigned to this feature
	 * @param pointWKT  Well-Known Text representation of the (point) geometry
	 */
	private void insertWGS84Point(String uri, String pointWKT) {
	    	
	    //Clean point WKT so as to retain its numeric coordinates only
	  	String[] parts = pointWKT.replace("POINT","").replace("(","").replace(")","").split(Constants.BLANK);
	  	
	  	//X-ordinate as a property
	  	createTriple4TypedLiteral(uri, Constants.NS_POS + Constants.LONGITUDE, parts[0], TypeMapper.getInstance().getSafeTypeByName(Constants.NS_XSD + "float"));
	  	 
	  	//Y-ordinate as a property
	  	createTriple4TypedLiteral(uri, Constants.NS_POS + Constants.LATITUDE, parts[1], TypeMapper.getInstance().getSafeTypeByName(Constants.NS_XSD + "float"));

	}

	
	/**
	 * Transforms all thematic (i.e., non-spatial) attributes according to a custom ontology specified in a YML format
	 * @param uri  The URI assigned to this feature
	 * @param attrValues  Attribute values for each thematic (non-spatial) attribute of the feature
	 * @param classific  The classification scheme used in the category assigned to the feature
	 * @throws UnsupportedEncodingException
	 */
	public void transformCustomThematic2RDF(String uri, Map<String, String> attrValues, Classification classific) throws UnsupportedEncodingException  {    
		
  	    try 
  	    {
  	    	Set<String> indexCompAttrs = new HashSet<String>();      //Retains an index for all composite entities consisting of multiple attributes (e.g., address)
  	        
  	        //Include a category identifier, as found in the classification scheme and suffixed with the user-specified namespace
	      	if ((classific != null) && (classific.getUUID(attrValues.get(currentConfig.attrCategory))) != null)
	      		attrValues.put(attrCategoryURI, currentConfig.featureClassNS + classific.getUUID(attrValues.get(currentConfig.attrCategory)));
	      	
	      	//Also include information about the data source provider as specified in the configuration
	      	attrValues.put(attrDataSource, currentConfig.featureSource);
	      	
  	        //Iterate over each attribute specified in the mapping and insert triple(s) according to its specifications
  	        for (String key: attrValues.keySet())
  	        {
  	        	if (!key.equals(currentConfig.attrGeometry))    // (!key.equals(currentConfig.attrKey))
  	        	{
  	        		String val = attrValues.get(key);
  	        		if ((val != null) && (!val.equals("")) && (!val.contains("Null")))       //Issue triples for NOT NULL/non-empty values only
  	        		{
  	        			val = val.replaceAll("[\\\t|\\\n|\\\r]"," ");     //FIXME: Replacing newlines with space, but other special characters should be handled as well
  	        			
  	        			mapProperties mapping = attrMappings.find(key);   //Mapping associated with this attribute
  	        			if (mapping == null)               //Cannot find a mapping for this attribute
  	        			{
  	        				//Emit any attribute not specifically mapped to the ontology as triples for (key, value) pairs
  	        				createTriple4Resource(uri, currentConfig.ontologyNS + "otherAttr", uri + "/" + key);
  	        				createTriple4PlainLiteral(uri + "/" + key, currentConfig.ontologyNS + "key", key);
  	        				createTriple4PlainLiteral(uri + "/" + key, currentConfig.ontologyNS + "value", val);
  	        				
  	        				continue;
  	        			}
    			
  	        			updateStatistics(key);                          //Update count of NOT NULL values transformed for this attribute
  	        			
  	        			//User specifications for transforming this attribute
  	        			String resPart = mapping.getPart();             //This resource is part of another entity (e.g., streetname is part of address)
  	        			String resClass = mapping.getInstance();        //This resource instantiates a class (e.g., email instantiates a contact)
  	        			String entityType = mapping.getEntityType();    //Entity type used as a suffix to the URI
        				String predicate = mapping.getPredicate();      //Predicate according to the ontology
        				String resType = mapping.getResourceType();     //Type of the resource                 
        				String lang = mapping.getLanguage();            //Language used in string literals
        				RDFDatatype dataType = mapping.getDataType();   //Data type for literals
        				
        				//Handle value for this attribute according to its designated mapping profile
        				switch (mapping.getMappingProfile()) {
        					case IS_INSTANCE_TAG_LANGUAGE :       //Property is an instance of class in the ontology and also specifies language tag in literals 
        						createTriple4Resource(uri, predicate, uri + "/" + entityType);
        						createTriple4LanguageLiteral(uri + "/" + entityType, currentConfig.ontologyNS + resClass + "Value", val, lang);
        						createTriple4PlainLiteral(uri + "/" + entityType, currentConfig.ontologyNS + "language", lang);
        						createTriple4PlainLiteral(uri + "/" + entityType, currentConfig.ontologyNS + resClass + "Type", resType); 
        						//Also insert a triple for the RDF class of this entity
        						createTriple4Resource(uri + "/" + entityType, RDF.type.getURI(), currentConfig.ontologyNS + resClass);
        						break;
        					case IS_INSTANCE :                    //Property is an instance of class in the ontology without language tags
        						createTriple4Resource(uri, predicate, uri + "/" + entityType);
            					createTriple4PlainLiteral(uri + "/" + entityType, currentConfig.ontologyNS + resClass + "Value", val);
            					createTriple4PlainLiteral(uri + "/" + entityType, currentConfig.ontologyNS + resClass + "Type", resType); 
            					//Also insert a triple for the RDF class of this entity
            					createTriple4Resource(uri + "/" + entityType, RDF.type.getURI(), currentConfig.ontologyNS + resClass);
            					break;
        					case IS_PART_TAG_LANGUAGE :          //Property is part of a composite class in the ontology and also specifies language tag in literals 
        						if (!indexCompAttrs.contains(resPart))
            					{
            						createTriple4Resource(uri, currentConfig.ontologyNS + entityType, uri + "/" + resPart);
            						indexCompAttrs.add(resPart);
            						//Also insert a triple for the RDF class of this entity
            						createTriple4Resource(uri + "/" + resPart, RDF.type.getURI(), currentConfig.ontologyNS + resPart);
            					}
            					createTriple4LanguageLiteral(uri + "/" + resPart, predicate, val, lang);
            					break;
        					case IS_PART :                        //Property is part of a composite class in the ontology without language tags
        						if (!indexCompAttrs.contains(resPart))
            					{
            						createTriple4Resource(uri, currentConfig.ontologyNS + entityType, uri + "/" + resPart);
            						indexCompAttrs.add(resPart);
            						//Also insert a triple for the RDF class of this entity
            						createTriple4Resource(uri + "/" + resPart, RDF.type.getURI(), currentConfig.ontologyNS + resPart);
            					}
            					createTriple4PlainLiteral(uri + "/" + resPart, predicate, val);
            					break;
        					case HAS_DATA_TYPE_URL :             //Property with a URL object; URLs must be valid
        						val = val.replaceAll("\\s+","");                  //Eliminate white spaces and invalid characters
        						val = val.replaceAll("|","");
        						if (!val.toLowerCase().matches("^\\w+://.*"))     //This value should be a URL, so put HTTP as its prefix
        							val = "http://" + val;
        						createTriple4Resource(uri, predicate, val);
        						break;
        					case HAS_DATA_TYPE :                  //Property with a literal having data type specification
        						createTriple4TypedLiteral(uri, expandNamespace(predicate), val, dataType);
        						break;
        					case IS_LITERAL :                     //Property with a plain literal without further specifications
        						createTriple4PlainLiteral(uri, predicate, val);
        						break;
        					default:                              //No action
        							
        				};			
  	        		}
        		}
        	}    
  	    }
  	    catch(Exception e) { 
  	    	ExceptionHandler.warn(e, " An error occurred when attempting transformation of a thematic attribute value.");
  	    }
	}

	
	/**
	 * Transforms a given category in a (possibly hierarchical) classification scheme into RDF triples
	 * FIXME: Current handling fits the classification scheme suggested by the SLIPO ontology for Points of Interest (POI) 
	 * @param uuid  A universally unique identifier (UUID) assigned to the category
	 * @param name  The name of this category according to the classification
	 * @param parent_uuid  The universally unique identifier (UUID) assigned to the parent of this category in the classification scheme
	 */
	public void transformCategory2RDF(String uuid, String name, String parent_uuid) {    	
	
  	    try 
  	    {	
	      	//Classification scheme is named according to the data source provider as specified in the configuration
  	    	String classificSource = currentConfig.featureSource;
  	    	
  	    	//Create an identifier for the RDF resource
  	        String encodingResource = URLEncoder.encode(uuid, Constants.UTF_8).replace(Constants.WHITESPACE, Constants.REPLACEMENT);	  	      
  	        String uri = currentConfig.featureClassNS + encodingResource;
	      	  	
  	        //Create triples
  	        createTriple4Resource(uri, currentConfig.ontologyNS + "termClassification", currentConfig.featureClassificationNS + classificSource);
  	        createTriple4Resource(uri, RDF.type.getURI(), currentConfig.ontologyNS + "Term");
  	        createTriple4PlainLiteral(uri, currentConfig.ontologyNS + "value", name); 
  	    	if (parent_uuid != null)
  	    		createTriple4Resource(uri, currentConfig.ontologyNS + "parent", currentConfig.featureClassNS + URLEncoder.encode(parent_uuid, Constants.UTF_8).replace(Constants.WHITESPACE, Constants.REPLACEMENT));	  
  	    }
  	    catch(Exception e) { 
  	    	ExceptionHandler.warn(e, " An error occurred when attempting transformation of a thematic attribute value.");
  	    }	    
	}

	
	/**
	 * Expands the prefix into the full namespace of a given RDF node (usually, a predicate)
	 * @param s  A prefixed name with a prefix label and a local part, separated by a colon ":"
	 * @return  A URI by concatenating the expanded namespace associated with the prefix and the local part
	 */
	private String expandNamespace(String s) {
		
		String prefix = s.substring(0, s.indexOf(':'));  //Get the prefix
		String namespace = prefixes.get(prefix);         //Identify its respective full namespace
		if (namespace != null)
			return s.replace(prefix + ":", namespace);   //... and replace it
		
		return s;	 //No replacement took place
	}


	/**
	 * Creates an RDF triple with specific handling of literals having a language tag
	 * @param s  Triple subject
	 * @param p  Triple predicate
	 * @param o  Triple object literal
	 * @param lang -- Language specification of the literal value
	 */
	private void createTriple4LanguageLiteral(String s, String p, String o, String lang) { 
		
	    results.add(new Triple(NodeFactory.createURI(s), NodeFactory.createURI(expandNamespace(p)), NodeFactory.createLiteral(o, lang)));
	}

	
	/**
	 * Creates an RDF triple for a plain literal (without language tag or data type specification)
	 * @param s -- Triple subject
	 * @param p -- Triple predicate
	 * @param o -- Triple object literal
	 */
	private void createTriple4PlainLiteral(String s, String p, String o) { 
		
	    results.add(new Triple(NodeFactory.createURI(s), NodeFactory.createURI(expandNamespace(p)), NodeFactory.createLiteral(o)));
	}

	/**
	 * Creates an RDF triple with a resource as its object (i.e., non literal values)
	 * @param s -- Triple subject
	 * @param p -- Triple predicate
	 * @param o -- Triple object resource
	 */
	private void createTriple4Resource(String s, String p, String o) { 
		
	    results.add(new Triple(NodeFactory.createURI(s), NodeFactory.createURI(expandNamespace(p)), NodeFactory.createURI(o)));
	}
	

	/**
	 * Creates an RDF triple with specific handling of literals having a data type specification
	 * @param s -- Triple subject
	 * @param p -- Triple predicate
	 * @param o -- Triple object literal
	 * @param d -- Data type specification of the literal value
	 */
	private void createTriple4TypedLiteral(String s, String p, String o, RDFDatatype d) { 

	    results.add(new Triple(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createLiteral(o, d)));
	}
	
}
