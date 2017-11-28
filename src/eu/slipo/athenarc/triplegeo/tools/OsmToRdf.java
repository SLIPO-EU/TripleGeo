/*
 * @(#) OsmToRdf.java	version 1.3   28/11/2017
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
package eu.slipo.athenarc.triplegeo.tools;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
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
 * Entry point to convert OpenStreetMap (OSM) XML files into RDF triples.
 * CAUTION: Current limitation in the transformation process: due to in-memory indexing of OSM features, this process can handle only a moderate amount of OSM features. 
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 19/4/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */
public class OsmToRdf extends DefaultHandler {

	  Converter myConverter;
	  Assistant myAssistant;
	  public int sourceSRID;                //Source CRS according to EPSG 
	  public int targetSRID;                //Target CRS according to EPSG
	  private Configuration currentConfig;  //User-specified configuration settings
	  private String inputFile;             //Input OSM file
	  private String outputFile;            //Output RDF file
	
	  private boolean modeStream = false;   //Indicates that a stream-based (in-memory) conversion to RDF will be employed
	  StreamRDF stream;                     //Used in the stream-based (in-memory) conversion to RDF
  
	    int numRec;
	    int numTriples;
	    long numNodes;
	    long numWays;
	    long numRelations;
	    long numNamedEntities;

	    private GeometryFactory geometryFactory = new GeometryFactory();
	    
	    //Using Native Java objects for in-memory maintenance of dictionaries
	    private Map<String, Geometry> nodeIndex;      //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMNode objects   
	    private Map<String, Geometry> wayIndex;       //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMWay objects    
	    private Map<String, Geometry> relationIndex;  //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMRelation objects

	    private OSMNode nodeTmp;                             //the current OSM node object
	    private OSMWay wayTmp;                               //the current OSM way object
	    private OSMRelation relationTmp;                     //the current OSM relation object
	    
	    private static List<OSMFilter> filters;              //Parser for a file with filters for assigning categories to OSM features
	    
	    private boolean inWay = false;                       //when parser is in a way node becomes true in order to track the parser position 
	    private boolean inNode = false;                      //becomes true when the parser is in a simple node        
	    private boolean inRelation = false;                  //becomes true when the parser is in a relation node
	
	    
	  //Class constructor
	  public OsmToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) {
       
		  currentConfig = config;
		  inputFile = inFile;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;         //Currently assuming that both OSM input and RDF output will be georeferenced in WGS84
	      this.targetSRID = targetSRID;         //..., so no transformation is needed
	      myAssistant = new Assistant();
	      
	      try {
	    	  System.out.println(Constants.OSMPOISPBF_COPYRIGHT);
		      //Get filter definitions over combinations of OSM tags in order to determine POI categories
		      //Read YML file from configuration settings containing assignment of OSM tags to categories
		      OSMFilterFileParser filterFileParser = new OSMFilterFileParser(config.classificationSpec);    
		      filters = filterFileParser.parse();   //Parse the file containing filters for assigning categories to OSM features
	      }
		  catch(Exception e) { 
				ExceptionHandler.invoke(e, "Cannot initialize parser for OSM data. Missing or malformed YML file with classification of OSM tags into categories.");
		  }
	      
	      // Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }
	  }

	  //Assign a category to the OSM feature based on its tags
		private static String getCategory(Map<String, String> tags) {
			// Iterate filters
			String cat = null;
			for(OSMFilter filter : filters) {
				cat = getCategoryRecursive(filter, tags, null);
				if(cat != null) {
					return cat;
				}
			}
			return null;
		}

		private static String getCategoryRecursive(OSMFilter filter, Map<String, String> tags, String key) {
			// Use key of parent rule or current
			if(filter.hasKey()) {
				key = filter.getKey();
			}

			// Check for key/value
			if(tags.containsKey(key)) {
				if(filter.hasValue() && !filter.getValue().equals(tags.get(key))) {
					return null;
				}
			} else {
				return null;
			}

			// If children have categories, those will be used
			for(OSMFilter child : filter.childs) {
				String cat = getCategoryRecursive(child, tags, key);
				if(cat != null) {
					return cat;
				}
			}
			return filter.getCategory();
	}
		

	    //Call Saxon to parse the XML file
	    public void parseDocument() {
	    	
	    	//OPTION #1: Memory-based native Java structures for indexing
	    	nodeIndex = new HashMap<>(); 
	        wayIndex = new HashMap<>();
	        relationIndex = new HashMap<>();
	        
	        //Invoke XSL transformation against input OSM XML file
	        System.out.println("Calling parser for OSM XML file...");
	        SAXParserFactory factory = SAXParserFactory.newInstance();
	        try {
	            SAXParser parser = factory.newSAXParser();
	            parser.parse(inputFile, this);
	        } catch (ParserConfigurationException e) {
	        	ExceptionHandler.invoke(e, "Parser Configuration error.");
	        } catch (SAXException e) {
	        	ExceptionHandler.invoke(e, "SAXException : OSM xml not well formed." );
	        } catch (IOException e) {
	        	ExceptionHandler.invoke(e, "Cannot access input file.");
	        }
	        
	    }

	    
	    @Override
	    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
	    
	    	try {
		        // if current element is an OSMNode, 
		        if (elementName.equalsIgnoreCase("node")) {         //Create a new OSM node object and populate it with the appropriate values
		            nodeTmp = new OSMNode();
		            nodeTmp.setID(attributes.getValue("id"));
	
		            //parse geometry
		            double longitude = Double.parseDouble(attributes.getValue("lon"));
		            double latitude = Double.parseDouble(attributes.getValue("lat"));
		           
		            //create geometry object with original WGS84 coordinates
		            Geometry geom = geometryFactory.createPoint(new Coordinate(longitude, latitude));
		            nodeTmp.setGeometry(geom);
		            inNode = true;
		            inWay = false;
		            inRelation = false;
		        } 
		        else if (elementName.equalsIgnoreCase("way")) {   //create a new OSM way object and populate it with the appropriate values
		            wayTmp = new OSMWay();
		            wayTmp.setID(attributes.getValue("id"));
		            inWay = true;
		            inNode = false;
		            inRelation = false;
		        } 
		        else if (elementName.equalsIgnoreCase("relation")) {   //create a new OSM relation and populate it with the appropriate values
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
		                //if the path is in an OSM node, then set tagKey and value to the corresponding node     
		                nodeTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            } 
		            else if (inWay) {
		                //else if the path is in an OSM way, then set tagKey and value to the corresponding way
		                wayTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            } 
		            else if(inRelation){
		                //set the key-value pairs of OSM relation tags
		                relationTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
		            }           
		        } 
		        else if (elementName.equalsIgnoreCase("member")) {
		            relationTmp.addMemberReference(attributes.getValue("ref"), attributes.getValue("role"));
		        }  
	    	}
	    	catch (Exception e) {
	        	ExceptionHandler.invoke(e, "Cannot process OSM element.");
	        }
	    	
	    }

	    @Override
	    public void endElement(String s, String s1, String element) {
	    	
	    	try
	    	{
		        // if end of node element, add to appropriate list
		        if (element.equalsIgnoreCase("node")) {
		            if (nodeTmp.getTagKeyValue().containsKey("name"))
		            	convertRecord(getOSMRecord(nodeTmp));
		            nodeIndex.put(nodeTmp.getID(), nodeTmp.getGeometry());         //Keep a dictionary of all node geometries, just in case they might be needed to handle OSM relations
		            numNodes++;
		        }
		        
		        if (element.equalsIgnoreCase("way")) {            
		            
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
		            	convertRecord(getOSMRecord(wayTmp));
		            wayIndex.put(wayTmp.getID(), wayTmp.getGeometry());          //Keep a dictionary of all way geometries, just in case they might be needed to handle OSM relations
		            numWays++;
		        } 
		        
		        if(element.equalsIgnoreCase("relation")) {
		        	OSMRecord rec = getOSMRecord(relationTmp);
		            if (relationTmp.getTagKeyValue().containsKey("name"))
		            	convertRecord(rec);
		            relationIndex.put(relationTmp.getID(), rec.getGeometry());    //Keep a dictionary of all relation geometries, just in case they might be needed to handle other OSM relations
		            numRelations++;
		        }
	    	}
	    	catch (Exception e) {
	        	ExceptionHandler.invoke(e, "Cannot process OSM element.");
	        }
	    }

	    
	  
	/*
	 * Apply transformation according to configuration settings.
	 */  
	public void apply() {

	    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	    numRec = 0;
	    numTriples = 0;
	    numNodes = 0;
	    numWays = 0;
	    numRelations = 0;
	    numNamedEntities = 0;
	        
	      try {			
				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig);
				
				  //Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
				  parseDocument();
				 			  
				 //Measure parsing time including cost for construction of the on-disk RDF model
				 dt = System.currentTimeMillis() - t_start;
				 System.out.println(myAssistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
				 System.out.println(myAssistant.getGMTime() + " Starting to write triples to file...");
				    
				 //Count the number of statements
				 numTriples = myConverter.getModel().listStatements().toSet().size();
				  
				    //Export RDF model to a suitable format 
				    try {
				    	FileOutputStream out = new FileOutputStream(outputFile);
				    	myConverter.getModel().write(out, currentConfig.serialization);  
				    }
				    catch(Exception e) { 
				    	ExceptionHandler.invoke(e, "Serialized output cannot be written into a file. Please check configuration file.");
				    }		    
				    
				  //Remove all temporary files as soon as processing is finished
				    myAssistant.removeDirectory(currentConfig.tmpDir);
				}
				else					
				{					
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);  
				  modeStream = true;
				  
				  OutputStream outFile = null;
				  try {
						outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
				  } 
				  catch (FileNotFoundException e) {
					  ExceptionHandler.invoke(e, "Output file not specified correctly.");
				  } 
					
				  //CAUTION! Hard constraint: serialization into N-TRIPLES is only supported by Jena riot (stream) interface
				  stream = StreamRDFWriter.getWriterStream(outFile, RDFFormat.NTRIPLES);   //StreamRDFWriter.getWriterStream(os, Lang.NT);					
				  stream.start();             //Start issuing streaming triples
				  
				  //Parse each OSM entity and streamline the resulting triples (including geometric and non-spatial attributes)
				  parseDocument();
				  						
				  stream.finish();               //Finished issuing triples
				    
				}
	      } catch (Exception e) {
	    	  ExceptionHandler.invoke(e, "");
	  	  }
	      
		  //Measure execution time
		  dt = System.currentTimeMillis() - t_start;
		  System.out.println(myAssistant.getGMTime() + " Parsing completed. Original OSM file contains: " + numNodes + " nodes, " + numWays + " ways, " + numRelations + " relations. In total, " + numNamedEntities + " entities had a name and only those were given as input to transformation.");
		  myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
  
	}
	

	//Convert OSMRecord object (node, way, or relation) into RDF triples
	private void convertRecord(OSMRecord rs) {
		try {
			numNamedEntities++;
			
			String wkt = rs.getGeometry().toText();   //Get WKT representation
	      	
			//Parse geometric representation
	      	myConverter.parseGeom2RDF(currentConfig.featureName, rs.getID(), wkt, targetSRID);
	      	
			//Process non-spatial attributes for name and type
			if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))		
					myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.getID(), rs.getName(), rs.getCategory());

			//In case of a streaming mode, immediately convert record into triples
			if (modeStream) {
				
				  //Append each triple to the output stream 
				  for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
						stream.triple(myConverter.getTriples().get(i));
						numTriples++;
					}
				  
				  myConverter =  new StreamConverter(currentConfig);   //Clear emitted triples from the converter in order to get those of the next record
			}
			
			myAssistant.notifyProgress(++numRec);
				
		} catch (Exception e) {
			System.out.println("Problem at element with OSM id: " + rs.getID() + ". Excluded from transformation.");
		}		
	}
  
	
  //Construct an OSMRecord structure from a parsed OSM node
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
  
  
  //Construct an OSMRecord structure from a parsed OSM way
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
  
  //Examine all way objects and check whether they can construct a new ring or update an existing one
  //Returns the amount of rings (internal or external, depending on call) for the given feature
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
  
  //Construct an OSMRecord structure from a parsed OSM relation
  //CAUTION: Sometimes, this process may yield topologically invalid geometries, because of irregularities (e.g., self-intersections) tolerated by OSM!
  private OSMRecord getOSMRecord(OSMRelation r) {

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
//	    			else
//	    				System.out.println("There is no OSM item with id: " + member);
	    		}
				
				if (numMembers == 1)            //In case that this relation consists of one member only, just replicate the original geometry
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
		else if ((rec.getGeometry() == null) || (!rec.getGeometry().isValid()))
			//System.out.println(rec.getID() + ";" + rec.getGeometry().toString());
			System.out.println("Geometry is extracted, but it is not valid for OSM id = " + rec.getID() + ".");
   	
    	return rec;
  }
  
}
