/*
 * @(#) OsmToRdf.java	version 1.4   28/2/2018
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
package eu.slipo.athenarc.triplegeo.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.linemerge.LineMerger;

import eu.slipo.athenarc.triplegeo.osm.OSMFilter;
import eu.slipo.athenarc.triplegeo.osm.OSMFilterFileParser;
import eu.slipo.athenarc.triplegeo.osm.OSMNode;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;
import eu.slipo.athenarc.triplegeo.osm.OSMRelation;
import eu.slipo.athenarc.triplegeo.osm.OSMWay;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Entry point to convert OpenStreetMap (OSM) XML files into RDF triples using Saxon XSLT.
 * LIMITATIONS: - Due to in-memory indexing of OSM features, transformation can handle only a moderate amount of OSM features depending on system and JVM resources.
 *              - RML transformation mode not supported. 
 * @author Kostas Patroumpas
 * @version 1.4
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 19/4/2017
 * Modified: 7/9/2017; added filters for tags in order to assign categories to extracted OSM features according to a user-specified classification scheme (defined in an extra YML file).
 * Modified: 3/11/2017; added support for system exit codes on abnormal termination
 * Modified: 19/12/2017; reorganized collection of triples using TripleGenerator
 * Modified: 24/1/2018; included auto-generation of UUIDs for the URIs of features
 * Modified: 7/2/2018; added support for exporting all available non-spatial attributes as properties
 * Modified: 12/2/2018; added support for reprojection to another CRS
 * Modified: 13/2/2018; handling incomplete OSM relations in a second pass after the XML file is parsed in its entirety
 * TODO: Emit RDF triples regarding the classification scheme utilized in assigning categories to OSM elements.
 * Last modified by: Kostas Patroumpas, 28/2/2018
 */
public class OsmToRdf extends DefaultHandler {

	  Converter myConverter;
	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  int sourceSRID;                       //Source CRS according to EPSG 
	  int targetSRID;                       //Target CRS according to EPSG
	  private Configuration currentConfig;  //User-specified configuration settings
	  private String inputFile;             //Input OSM XML file
	  private String outputFile;            //Output RDF file
	
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	  long numNodes;
	  long numWays;
	  long numRelations;
	  long numNamedEntities;

	  private GeometryFactory geometryFactory = new GeometryFactory();
	    
	  //Using Native Java objects for in-memory maintenance of dictionaries
	  private Map<String, Geometry> nodeIndex;      //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMNode objects   
	  private Map<String, Geometry> wayIndex;       //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMWay objects    
	  private Map<String, Geometry> relationIndex;  //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMRelation objects

	  private List<OSMRelation> incompleteRelations;
	    
	  private OSMNode nodeTmp;                             //the current OSM node object
	  private OSMWay wayTmp;                               //the current OSM way object
	  private OSMRelation relationTmp;                     //the current OSM relation object
	    
	  private static List<OSMFilter> filters;              //Parser for a file with filters for assigning categories to OSM features
	    
	  private boolean inWay = false;                       //when parser is in a way node becomes true in order to track the parser position 
	  private boolean inNode = false;                      //becomes true when the parser is in a simple node        
	  private boolean inRelation = false;                  //becomes true when the parser is in a relation node
	

	  /**
	   * Constructor for the transformation process from OpenStreetMap XML file to RDF.
	   * @param config  Parameters to configure the transformation.
	   * @param inFile  Path to input OSM XML file.
	   * @param outFile  Path to the output file that collects RDF triples.
	   * @param sourceSRID  Spatial reference system (EPSG code) of the input OSM XML file.
	   * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	   */
	  public OsmToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) {
       
		  currentConfig = config;
		  inputFile = inFile;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;                      //Assume that OSM input is georeferenced in WGS84
	      this.targetSRID = targetSRID;
	      myAssistant = new Assistant();
	      
	      //Get filter definitions over combinations of OSM tags in order to determine POI categories
	      try {
	    	  System.out.println(Constants.OSMPOISPBF_COPYRIGHT);
		      //Read YML file from configuration settings containing assignment of OSM tags to categories
		      OSMFilterFileParser filterFileParser = new OSMFilterFileParser(config.classificationSpec);    
		      filters = filterFileParser.parse();   //Parse the file containing filters for assigning categories to OSM features
		      if (filters == null)
		    	  throw new FileNotFoundException(config.classificationSpec);
	      }
		  catch(Exception e) { 
				ExceptionHandler.abort(e, "Cannot initialize parser for OSM data. Missing or malformed YML file with classification of OSM tags into categories.");
		  }

	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.targetCRS != null)
	      {
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
	      else    //No transformation specified; determine the CRS of geometries...
	      {
	    		  this.targetSRID = 4326;          //... as the original OSM features assumed in WGS84 lon/lat coordinates
	    		  System.out.println(Constants.WGS84_PROJECTION);
	      }
		    
	      //Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }
	  }

		
	  /**
	   * Assign a category to a OSM feature (node, way, or relation) based on its tags.
	   * @param tags  Key-value pairs for OSM tags and their respective values for a given feature.
	   * @return  A category according to the classification scheme based on OSM tags.
	   */
	  private static String getCategory(Map<String, String> tags) {
		//Iterate over filters
		String cat = null;
		for (OSMFilter filter : filters) {
			cat = getCategoryRecursive(filter, tags, null);
			if (cat != null) {
				return cat;
			}
		}
		return null;
	  }



	  /**
	   * Recursively search in the classification hierarchy to match the given tag (key).
	   * @param filter  Correspondence of OSM tags into categories.
	   * @param tags  Key-value pairs for OSM tags and their respective values for a given feature.
	   * @param key  Tag at any level in the classification hierarchy.
	   * @return
	   */
	  private static String getCategoryRecursive(OSMFilter filter, Map<String, String> tags, String key) {
			//Use key of parent rule or current
			if (filter.hasKey()) {
				key = filter.getKey();
			}

			//Check for key/value
			if (tags.containsKey(key))
				if (filter.hasValue() && !filter.getValue().equals(tags.get(key)))
					return null;
			else
				return null;

			//If children have categories, those will be used
			for (OSMFilter child : filter.childs)
			{
				String cat = getCategoryRecursive(child, tags, key);
				if (cat != null) 
					return cat;
			}
			return filter.getCategory();
	  }
		

	  /**
	   * Calls Saxon transformation to parse the input XML file.
	   */
	  public void parseDocument() {
	    	
	    	//OPTION #1: Memory-based native Java structures for indexing
	    	nodeIndex = new HashMap<>(); 
	        wayIndex = new HashMap<>();
	        relationIndex = new HashMap<>();
	        incompleteRelations = new ArrayList<>();
	        
	        //Invoke XSL transformation against input OSM XML file
	        System.out.println("Calling parser for OSM XML file...");
	        SAXParserFactory factory = SAXParserFactory.newInstance();
	        try {
	            SAXParser parser = factory.newSAXParser();
	            parser.parse(inputFile, this);
	            
	            //Second pass over incomplete OSM relations, once the entire XML file has been parsed
	            for (Iterator<OSMRelation> iterator = incompleteRelations.iterator(); iterator.hasNext(); )
	            {
	            	OSMRelation r = iterator.next();
	            	System.out.print("Re-examining OSM relation " + r.getID() + "...");
	            	OSMRecord rec = getOSMRecord(r);
	            	if (rec != null)                    //Incomplete relations are accepted in this second pass, consisting of their recognized parts   
	            	{
	            		if (r.getTagKeyValue().containsKey("name"))
	            		{
	            			myConverter.parse(myAssistant, rec, reproject, targetSRID);
	            			numNamedEntities++;
	            		}
	            		relationIndex.put(r.getID(), rec.getGeometry());    //Keep a dictionary of all relation geometries, just in case they might be needed to handle other OSM relations
	            		numRelations++;
	            		iterator.remove();                                  //This OSM relation should not be examined again
	            		System.out.println(" Done!");
	            	} 
	            	else
	            		System.out.println(" Transformation failed!");
	            }
	        } catch (ParserConfigurationException e) {
	        	ExceptionHandler.abort(e, "Parser Configuration error.");
	        } catch (SAXException e) {
	        	ExceptionHandler.abort(e, "SAXException : OSM xml not well formed." );
	        } catch (IOException e) {
	        	ExceptionHandler.abort(e, "Cannot access input file.");
	        }
	        
	    }

	    /**
	     * Starts parsing of a new OSM element (node, way, or relation). Overrides method in parent Saxon class to initialize variables at the start of parsing of each OSM element.
	     * @param s  he Namespace URI, or the empty string if the element has no Namespace URI or if Namespace processing is not being performed.
	     * @param s1  The local name (without prefix), or the empty string if Namespace processing is not being performed.
	     * @param elementName  The qualified name (with prefix), or the empty string if qualified names are not available.
	     * @param attributes   The attributes attached to the element. If there are no attributes, it shall be an empty Attributes object.
	     */
	    @Override
	    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
	    
	    	try {
		        //Depending on the name of the current OSM element,... 
		        if (elementName.equalsIgnoreCase("node")) {           //Create a new OSM node object and populate it with the appropriate values
		            nodeTmp = new OSMNode();
		            nodeTmp.setID(attributes.getValue("id"));
	
		            //Parse geometry
		            double longitude = Double.parseDouble(attributes.getValue("lon"));
		            double latitude = Double.parseDouble(attributes.getValue("lat"));
		           
		            //Create geometry object with original WGS84 coordinates
		            Geometry geom = geometryFactory.createPoint(new Coordinate(longitude, latitude));
		            nodeTmp.setGeometry(geom);
		            inNode = true;
		            inWay = false;
		            inRelation = false;
		        } 
		        else if (elementName.equalsIgnoreCase("way")) {       //Create a new OSM way object and populate it with the appropriate values
		            wayTmp = new OSMWay();
		            wayTmp.setID(attributes.getValue("id"));
		            inWay = true;
		            inNode = false;
		            inRelation = false;
		        } 
		        else if (elementName.equalsIgnoreCase("relation")) {   //Create a new OSM relation and populate it with the appropriate values
		            relationTmp = new OSMRelation();
		            relationTmp.setID(attributes.getValue("id"));
		            inRelation = true;
		            inWay = false;
		            inNode = false;
		        } 
		        else if (elementName.equalsIgnoreCase("nd")) {
		            wayTmp.addNodeReference(attributes.getValue("ref"));
		        } 
		        else if (elementName.equalsIgnoreCase("tag")) {
		            if (inNode) {
		                //If the path is in an OSM node, then set tagKey and value to the corresponding node     
		                nodeTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            } 
		            else if (inWay) {
		                //Otherwise, if the path is in an OSM way, then set tagKey and value to the corresponding way
		                wayTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            } 
		            else if(inRelation){
		                //Set the key-value pairs of OSM relation tags
		                relationTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            }           
		        } 
		        else if (elementName.equalsIgnoreCase("member")) {
		            relationTmp.addMemberReference(attributes.getValue("ref"), attributes.getValue("role"));
		        }  
	    	}
	    	catch (Exception e) {
	        	ExceptionHandler.warn(e, "Cannot process OSM element.");
	        }    	
	    }

	    /**
	     * Concludes processing of an OSM element (node, way, or relation) once it has been parsed completely. Overrides method in the parent Saxon class to finalize variables and indices at the end of parsing of each OSM element.
	     * @param s  The Namespace URI, or the empty string if the element has no Namespace URI or if Namespace processing is not being performed.
	     * @param sl  The local name (without prefix), or the empty string if Namespace processing is not being performed.
	     * @param element   The qualified name (with prefix), or the empty string if qualified names are not available.
	     */
	    @Override
	    public void endElement(String s, String s1, String element) {
	    	
	    	try
	    	{
		        //If end of node element, add to appropriate list
		        if (element.equalsIgnoreCase("node")) {                            //OSM node
		            if (nodeTmp.getTagKeyValue().containsKey("name"))
		            {
		            	myConverter.parse(myAssistant, getOSMRecord(nodeTmp), reproject, targetSRID);
		            	numNamedEntities++;
		            }
		            nodeIndex.put(nodeTmp.getID(), nodeTmp.getGeometry());         //Keep a dictionary of all node geometries, just in case they might be needed to handle OSM relations
		            numNodes++;
		        } 
		        else if (element.equalsIgnoreCase("way")) {                        //OSM way      
		            
		            //construct the Way geometry from each node of the node references
		            List<String> references = wayTmp.getNodeReferences();
	
		            for (String entry: references) {
		               Geometry geometry = nodeIndex.get(entry);     //get the geometry of the node with ID=entry
		               wayTmp.addNodeGeometry(geometry);             //add the node geometry in this way  
		            }
		            Geometry geom = geometryFactory.buildGeometry(wayTmp.getNodeGeometries());
		            
		            if((wayTmp.getNumberOfNodes() > 3) && wayTmp.getNodeGeometries().get(0).equals(wayTmp.getNodeGeometries().get(wayTmp.getNodeGeometries().size()-1)))   {
		            //checks if the beginning and ending node are the same and the number of nodes are more than 3. 
		            //These nodes must be more than 3, because JTS does not allow construction of a linear ring with less than 3 points
		                
		               if (!((wayTmp.getTagKeyValue().containsKey("barrier")) || wayTmp.getTagKeyValue().containsKey("highway"))){
		            	   //this is not a barrier nor a road, so construct a polygon geometry
		               
		            	   LinearRing linear = geometryFactory.createLinearRing(geom.getCoordinates());
		            	   Polygon poly = new Polygon(linear, null, geometryFactory);
		            	   wayTmp.setGeometry(poly);               
		               }
		               else {    //it is either a barrier or a road, so construct a linear ring geometry 
		                  LinearRing linear = geometryFactory.createLinearRing(geom.getCoordinates());
		                  wayTmp.setGeometry(linear);  
		               }
		            }
		            else if (wayTmp.getNumberOfNodes() > 1) {
		            //it is an open geometry with more than one nodes, make it linestring 
		                
		                LineString lineString =  geometryFactory.createLineString(geom.getCoordinates());
		                wayTmp.setGeometry(lineString);               
		            }
		            else {      //we assume all the rest geometries are points
		                       //some ways happen to have only one point. Construct a Point.
		                Point point = geometryFactory.createPoint(geom.getCoordinate());
		                wayTmp.setGeometry(point);
		            }
		            
		            if (wayTmp.getTagKeyValue().containsKey("name"))  
		            {
		            	myConverter.parse(myAssistant, getOSMRecord(wayTmp), reproject, targetSRID);
		            	numNamedEntities++;
		            }
		            wayIndex.put(wayTmp.getID(), wayTmp.getGeometry());          //Keep a dictionary of all way geometries, just in case they might be needed to handle OSM relations
		            numWays++;
		        } 	   
		        else if (element.equalsIgnoreCase("relation")) {                 //OSM relation
		        	OSMRecord rec = getOSMRecord(relationTmp);
		        	if (rec!= null)                  //No records created for incomplete relations during the first pass
		        	{
		        		if ((rec!= null) && (relationTmp.getTagKeyValue().containsKey("name")))
		        		{
		        			myConverter.parse(myAssistant, rec, reproject, targetSRID);
		        			numNamedEntities++;
		        		}
		        		relationIndex.put(relationTmp.getID(), rec.getGeometry());    //Keep a dictionary of all relation geometries, just in case they might be needed to handle other OSM relations
		        		numRelations++;
		        	}
		        }
	    	}
	    	catch (Exception e) {
	        	ExceptionHandler.warn(e, "Cannot process OSM element.");
	        }
	    }

  
	/**
	 * Applies transformation according to the configuration settings.
	 */
	public void apply() {

	  numNodes = 0;
	  numWays = 0;
	  numRelations = 0;
	  numNamedEntities = 0;
        
      try {			
			if (currentConfig.mode.contains("GRAPH"))
			{
			  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
			  myConverter = new GraphConverter(currentConfig, outputFile);
			
			  //Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
			  parseDocument();

			  //Export the RDF graph into a user-specified serialization
			  myConverter.store(myAssistant, outputFile);

			  //Remove all temporary files as soon as processing is finished
			  myAssistant.removeDirectory(myConverter.getTDBDir());
			}
			else if (currentConfig.mode.contains("STREAM"))					
			{					
			  //Mode STREAM: consume records and streamline them into a serialization file
			  myConverter =  new StreamConverter(currentConfig, outputFile);
		  
			  //Parse each OSM entity and streamline the resulting triples (including geometric and non-spatial attributes)
			  parseDocument();
			  
			  //Finalize the output RDF file
			  myConverter.store(myAssistant, outputFile);
			}
			else    //TODO: Implement method for handling transformation using RML mappings
			{
				System.out.println("Mode " + currentConfig.mode + " is currently not supported against OSM XML datasets.");
				throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
			}
			
      } catch (Exception e) {
    	  ExceptionHandler.abort(e, "");
  	  }

      System.out.println(myAssistant.getGMTime() + " Original OSM file contains: " + numNodes + " nodes, " + numWays + " ways, " + numRelations + " relations. In total, " + numNamedEntities + " entities had a name and only those were given as input to transformation.");      
	}
	 
	
	/**
	 * Constructs an OSMRecord object from a parsed OSM node.
	 * @param n  A parsed OSM node.
	 * @return  An OSMRecord object as a record with specific attributes extracted from the OSM node.
	 */
    private OSMRecord getOSMRecord(OSMNode n) {
  
		OSMRecord rec = new OSMRecord();
	  	rec.setID("N" + n.getID());
	  	rec.setType(n.getTagKeyValue().get("type"));
	  	rec.setName(n.getTagKeyValue().get("name"));
	  	rec.setGeometry(n.getGeometry());
	  	rec.setTags(n.getTagKeyValue());
	  	rec.setCategory(getCategory(n.getTagKeyValue()));       //Search among the user-specified filters in order to assign a category to this OSM feature

	  	return rec;  	
    }
  
  
    /**
     * Constructs an OSMRecord object from a parsed OSM way.
     * @param w  A parsed OSM way.
     * @return  An OSMRecord object as a record with specific attributes extracted from the OSM way.
     */
    private OSMRecord getOSMRecord(OSMWay w) {
  
		OSMRecord rec = new OSMRecord();
	  	rec.setID("W" + w.getID());
	  	rec.setType(w.getTagKeyValue().get("type"));
	  	rec.setName(w.getTagKeyValue().get("name"));
	  	rec.setGeometry(w.getGeometry());
	  	rec.setTags(w.getTagKeyValue());
	  	rec.setCategory(getCategory(w.getTagKeyValue()));       //Search among the user-specified filters in order to assign a category to this OSM feature
	  	
	  	return rec;
    }
  
  
    /**
     * Examines all OSM way objects and checks whether they can construct a new ring (closed polyline) or update an existing one.
     * @param ways  Array of all the OSM ways identified in the feature.
     * @param numWays Number of OSM ways identified.
     * @param rings  Array of all linear rings identified in the feature.
     * @param numRings  Number of linear rings identified.
     * @return  The amount of rings (internal or external, depending on call) identified in the given feature.
     */
    @SuppressWarnings({ "unchecked"})
    private int rearrangeRings(LineString[] ways, int numWays, LinearRing[] rings, int numRings) {

		  //First, concatenate any polylines with common endpoints
		  LineMerger merger = new LineMerger();
		    for (int i=0; i<numWays; i++)
		    	if (ways[i] != null)
		    		merger.add(ways[i]); 
		    
		  //Then, for each resulting polyline, create a ring if this is closed
		  Collection<LineString> mergedWays = merger.getMergedLineStrings();  
		
		  for (LineString curWay: mergedWays)  
			  if (curWay.isClosed())
			  {
				  rings[numRings] = geometryFactory.createLinearRing(curWay.getCoordinates());
			      numRings += 1;
			  }	
		 	
		  return numRings; 
    }

  
    /**
     * Constructs an OSMRecord object from a parsed OSM relation. 
     * CAUTION! Sometimes, this process may yield topologically invalid geometries, because of irregularities (e.g., self-intersections) tolerated by OSM!
     * @param r   A parsed OSM relation.
     * @return  An OSMRecord object as a record with specific attributes extracted from the OSM relation.
     */
    private OSMRecord getOSMRecord(OSMRelation r) {

	    boolean incomplete = false;                             //Marks an OSM relation as incomplete in order to re-parse it at the end of the process
  		OSMRecord rec = new OSMRecord();
    	rec.setID("R" + r.getID());
    	rec.setType(r.getTagKeyValue().get("type"));
    	rec.setName(r.getTagKeyValue().get("name"));
    	rec.setTags(r.getTagKeyValue());
    	rec.setCategory(getCategory(r.getTagKeyValue()));       //Search among the user-specified filters in order to assign a category to this OSM feature
    	
    	//Reconstruct geometry from relating with its elementary nodes and ways
    	GeometryFactory geometryFactory = new GeometryFactory();
    	
    	//Examine the member geometries of each relation and create a specific type of geometry: MultiLineString, MultiPolygon, or GeometryCollection
    	try {
			if (r.getTagKeyValue().get("type") != null)
			{
				if  (r.getTagKeyValue().get("type").equalsIgnoreCase("multilinestring"))  // || (r.getTagKeyValue().get("type").equalsIgnoreCase("route"))
				{   //Create a MultiLineString for OSM relation features that are either a 'multilinestring' or a 'route'
					LineString[] memberGeometries = new LineString[r.getMemberReferences().size()];
					int numMembers = 0;

					//Iterate through all members of this OSMRelation
					for (Map.Entry<String, String> member : r.getMemberReferences().entrySet())     //Handle ways
    	    		{	
	    				Geometry tmpWay = wayIndex.get(member.getKey());
	    				if (tmpWay != null)
    	    			{
	    					//If this member is a polygon, then convert it into a linestring first
	    					if (tmpWay.getClass() == com.vividsolutions.jts.geom.Polygon.class) {
	    						tmpWay = ((Polygon) tmpWay).getExteriorRing();
	    					}
	    					memberGeometries[numMembers] = (LineString) tmpWay;     //Reference to OSMWay geometry
	    					numMembers++;
    	    			}
    	    		}
					
					if (numMembers > 0) {
						LineString[] finalGeometries = new LineString[numMembers];
						java.lang.System.arraycopy(memberGeometries, 0, finalGeometries, 0, numMembers);
    					rec.setGeometry(geometryFactory.createMultiLineString(finalGeometries));
					}
				}
				else if ((r.getTagKeyValue().get("type").equalsIgnoreCase("multipolygon")) || (r.getTagKeyValue().get("type").equalsIgnoreCase("boundary")))
				{   //Create a (Multi)Polygon for OSM relation features that are either a 'multipolygon' or a 'boundary'
					LinearRing[] outerRings = new LinearRing[r.getMemberReferences().size()];
					LinearRing[] innerRings = new LinearRing[r.getMemberReferences().size()];
					LineString[] outerWays = new LineString[r.getMemberReferences().size()];
					LineString[] innerWays = new LineString[r.getMemberReferences().size()];

					int numInnerRings = 0;
					int numOuterRings = 0;
					int numInnerWays = 0;
					int numOuterWays = 0;
					
					//Iterate through all members of this OSMRelation
					for (Map.Entry<String, String> member : r.getMemberReferences().entrySet())   //Handle ways       
    	    		{	
						Geometry tmpWay = wayIndex.get(member.getKey());

    	    			if (tmpWay != null)
    	    			{
    	    				if (tmpWay.getClass() == com.vividsolutions.jts.geom.Polygon.class)
    	    				{
    	    					if (member.getValue().equalsIgnoreCase("inner"))
    		    				{ 
    	    						innerRings[numInnerRings] = (LinearRing) ((Polygon) tmpWay).getExteriorRing();        //This OSMWay geometry is a complete inner ring
    	    						numInnerRings++;
    	    						
    		    				}
    	    					else //if (member.getValue().equalsIgnoreCase("outer"))                                  //Outer may not always be specified for such OSM entity!
    	    					{
	    	    					outerRings[numOuterRings] = (LinearRing) ((Polygon) tmpWay).getExteriorRing();       //Reference to OSMWay geometry
			    					numOuterRings++;	
    	    					}
    	    				}
    	    				else if (tmpWay.getClass() == com.vividsolutions.jts.geom.LineString.class) 
    	    				{     	    				
    	    					if (((LineString) tmpWay).isClosed()) {
    	    						if (member.getValue().equalsIgnoreCase("inner")) 
    	    						{
    	    							innerRings[numInnerRings] = (LinearRing) tmpWay;        //This OSMWay geometry is a complete inner ring
        	    						numInnerRings++;
    	    						}
    	    						else //if (member.getValue().equalsIgnoreCase("outer"))     //Outer may not always be explicitly specified for such OSM entity!
    	    						{
	    	    						outerRings[numOuterRings] = (LinearRing) tmpWay;        //Reference to OSMWay geometry
	    	    						numOuterRings++;
    	    						}
    	    					}
    	    					else
    	    					{
    	    						if (member.getValue().equalsIgnoreCase("inner")) 
    	    						{
    	    							innerWays[numInnerWays] = (LineString) tmpWay;          //This OSMWay geometry is only a part of an inner ring
        	    						numInnerWays++;
    	    						}
    	    						else //if (member.getValue().equalsIgnoreCase("outer"))     //Outer may not always be explicitly specified for such OSM entity!
    	    						{
	    	    						outerWays[numOuterWays] = (LineString) tmpWay;          //This OSMWay geometry is only a part of an outer ring
	    	    						numOuterWays++;
    	    						}						
    	    					}
		    				}
    	    				else if (tmpWay.getClass() == com.vividsolutions.jts.geom.LinearRing.class)
    	    				{
    	    					if (member.getValue().equalsIgnoreCase("inner"))
    		    				{ 
    	    						innerRings[numInnerRings] = (LinearRing) tmpWay;        //This OSMWay geometry is an inner ring
    	    						numInnerRings++;
    	    						
    		    				}
    	    					else //if (member.getValue().equalsIgnoreCase("outer"))     //Outer may not always be explicitly specified for such OSM entity!
    	    					{
	    	    					outerRings[numOuterRings] = (LinearRing) tmpWay;        //Reference to OSMWay geometry
			    					numOuterRings++;	
    	    					}
    	    				}
//    	    				else
//    	    					System.out.println(rec.getID() + ": Geometry is neither a LINESTRING nor a POLYGON! " + tmpWay.toString());
    	    			}
    	    		}
					
					//Update INNER rings by merging their constituent linestrings
					numInnerRings = rearrangeRings(innerWays, numInnerWays, innerRings, numInnerRings);
					//Update OUTER rings by merging their constituent linestrings
					numOuterRings = rearrangeRings(outerWays, numOuterWays, outerRings, numOuterRings);
					
					//A polygon, possibly with hole(s)
					if (numOuterRings == 1)
					{
						LinearRing[] innerHoles = new LinearRing[numInnerRings];
						if (numInnerRings > 0)
							java.lang.System.arraycopy(innerRings, 0, innerHoles, 0, numInnerRings);
						else
							innerHoles = null;
						Polygon geomPolygon = geometryFactory.createPolygon(outerRings[0], innerHoles);
						rec.setGeometry(geomPolygon);
					}
					else              //A MultiPolygon consisting of multiple polygons (possibly with holes)
					{
						Polygon[] polygons = new Polygon[outerRings.length];
						int numMembers = 0;
				
						//Iterate through all constituent outer rings in order to create a number of polygons for this composite geometry
						for (int i=0; i<numOuterRings; i++)
							if (outerRings[i] != null)
							{
								//Identify if an inner ring is within an outer one
								LinearRing[] innerHoles = new LinearRing[numInnerRings];
								int k = 0;
								for (int j=0; j<numInnerRings; j++)
									if ((geometryFactory.createPolygon(outerRings[i],null)).contains(geometryFactory.createPolygon(innerRings[j],null)))
									{
										innerHoles[k] = innerRings[j];	
										k++;
									}
								
								//If at least one hole if found, create the respective polygon accordingly
								LinearRing[] finalHoles = new LinearRing[k];
								if (k > 0)
									java.lang.System.arraycopy(innerHoles, 0, finalHoles, 0, k);
								else
									finalHoles = null;
							
								polygons[numMembers] = geometryFactory.createPolygon(outerRings[i], finalHoles);
								numMembers++;
							}
						Polygon[] finalPolygons = new Polygon[numMembers];
						java.lang.System.arraycopy(polygons, 0, finalPolygons, 0, numMembers);
						MultiPolygon geomMultiPolygon = geometryFactory.createMultiPolygon(finalPolygons);
    					rec.setGeometry(geomMultiPolygon);
					}				
				}
			}
			
			if (rec.getGeometry() == null)   //For any other type of OSM relations, create a geometry collection
			{
				Geometry[] memberGeometries = new Geometry[r.getMemberReferences().size()];
				int numMembers = 0;
				//Iterate through all members of this OSMRelation
				for (Map.Entry<String, String> member : r.getMemberReferences().entrySet())    
	    		{	
					String k = member.getKey();
	    			if (wayIndex.get(k) != null)              //Handle ways
	    			{
		    				memberGeometries[numMembers] = wayIndex.get(k);     //Reference to OSMWay geometry
		    				numMembers++;
	    			}
	    			else if (nodeIndex.get(k) != null)     	//Handle nodes
	    			{	
							memberGeometries[numMembers] = nodeIndex.get(k);    //Reference to OSMNode geometry
		    				numMembers++;
	    			}
	    			else if (relationIndex.get(k) != null)     	//Handle relations
	    			{	
							memberGeometries[numMembers] = relationIndex.get(k);    //Reference to OSMRelation geometry
		    				numMembers++;
	    			}
	    			else                                           //Missing constituent geometries
	    			{
//	    				System.out.println("There is no OSM item with id: " + member);
	    				if (!incompleteRelations.contains(r))      //Add this relation when it is first encountered in the parsing
	    				{
	    					incompleteRelations.add(r);
	    					incomplete = true;                      //At least one constituent geometry is missing
	    				}
	    			}
	    		}
				 
				if (incomplete)                      //Incomplete relations will be given a second chance once the XML file is parsed in its entirety
					return null;
					//rec.setGeometry(null);
				else if (numMembers == 1)            //In case that this relation consists of one member only, just replicate the original geometry
					rec.setGeometry(memberGeometries[0]);
				else if (numMembers > 1)        //Otherwise, create a geometry collection
				{
    				GeometryCollection geomCollection = null;
    				geomCollection = geometryFactory.createGeometryCollection(java.util.Arrays.copyOfRange(memberGeometries, 0, numMembers));
    				rec.setGeometry(geomCollection);
	    		}
			}

    	} catch (Exception e) {
    		System.out.print("PROBLEM at " + rec.getID() + ". REASON: ");
			e.printStackTrace();
		}

		if (rec.getGeometry() == null)
			System.out.println("Geometry is null for OSM id = " + rec.getID() + ".");
		else if (!rec.getGeometry().isValid())
			//System.out.println(rec.getID() + ";" + rec.getGeometry().toString());
			System.out.println("Geometry is extracted, but it is not valid for OSM id = " + rec.getID() + ".");
   	
    	return rec;
    }
  
}
