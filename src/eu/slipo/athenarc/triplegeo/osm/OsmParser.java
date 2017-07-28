package eu.slipo.athenarc.triplegeo.osm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import eu.slipo.athenarc.triplegeo.osm.OSMNode;
import eu.slipo.athenarc.triplegeo.osm.OSMRelation;
import eu.slipo.athenarc.triplegeo.osm.OSMWay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.opengis.referencing.FactoryException;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Parses OSM xml file and constructs additional nodes of the OSM map into appropriate objects with attributes.
 *
 * @author Nikos Karagiannakis
 * revised by: Kostas Patroumpas, 28/7/2017
 */

public class OsmParser extends DefaultHandler {

    //private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(OSMParser.class);
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final List<OSMNode> nodeList;                //to be populated with nodes of the OSM file
    private final List<OSMRelation> relationList;
    private final Map<String, Geometry> nodesWithIDs;    //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMNode objects   
    private final Map<String, Geometry> waysWithIDs;     //Dictionary containing OSM IDs as keys and the corresponding geometries of OSMWay objects
    private final List<OSMWay> wayList;                  //to populated with ways of the OSM file
    private final String osmXmlFileName;
    private OSMNode nodeTmp;                             //the current OSM node object
    private OSMWay wayTmp;                               //the current OSM way object
    private OSMRelation relationTmp;                     //the current OSM relation object
    private boolean inWay = false;                       //when parser is in a way node becomes true in order to track the parser position 
    private boolean inNode = false;                      //becomes true when the parser is in a simple node        
    private boolean inRelation = false;                  //becomes true when the parser is in a relation node
    
    
    public OsmParser(String osmXmlFileName) throws FactoryException {
        this.osmXmlFileName = osmXmlFileName;       
        nodeList = new ArrayList<>();
        wayList = new ArrayList<>();
        relationList = new ArrayList<>();
        nodesWithIDs = new HashMap<>(); 
        waysWithIDs = new HashMap<>();
    }

    public void parseDocument() {
        // parse
        System.out.println("Parsing OSM file...");
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = factory.newSAXParser();
            parser.parse(osmXmlFileName, this);
        } catch (ParserConfigurationException e) {
            System.out.println("ParserConfig error " + e);
        } catch (SAXException e) {
            System.out.println("SAXException : xml not well formed " + e);
        } catch (IOException e) {
            System.out.println("IO error " + e);
        }
    //LOG.info("Instances from OSM/XML file parsed!");    
    }

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
    
        // if current element is an OSMNode , 
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

        } else if (elementName.equalsIgnoreCase("way")) {   //create a new OSM way object and populate it with the appropriate values
            wayTmp = new OSMWay();
            wayTmp.setID(attributes.getValue("id"));
            inWay = true;
            inNode = false;
            inRelation = false;
        } else if (elementName.equalsIgnoreCase("relation")) {   //create a new OSM relation and populate it with the appropriate values
            relationTmp = new OSMRelation();
            relationTmp.setID(attributes.getValue("id"));
            inRelation = true;
            inWay = false;
            inNode = false;
        } else if (elementName.equalsIgnoreCase("nd")) {
            wayTmp.addNodeReference(attributes.getValue("ref"));

        } else if (elementName.equalsIgnoreCase("tag")) {

            if (inNode) {
                //if the path is in an OSMN node set tagKey and value to the corresponding node     
                nodeTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
            } else if (inWay) {
                //else if the path is in an OSM way set tagKey and value to the corresponding way
                wayTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
            } else if(inRelation){
                //set the key-value pairs of OSM relation tags
                relationTmp.setTagKeyValue(attributes.getValue("k"), attributes.getValue("v"));
            }           
        } else if (elementName.equalsIgnoreCase("member")) {
            relationTmp.addMemberReference(attributes.getValue("ref"));
        }                 
    }

    @Override
    public void endElement(String s, String s1, String element) throws SAXException {
        // if end of node element, add to appropriate list
        if (element.equalsIgnoreCase("node")) {
            nodeList.add(nodeTmp);
            nodesWithIDs.put(nodeTmp.getID(), nodeTmp.getGeometry());         //Keep a dictionary of all node geometries, just in case they might be needed to handle OSM relations
        }
        
        if (element.equalsIgnoreCase("way")) {            
            
            //construct the Way geometry from each node of the node references
            List<String> references = wayTmp.getNodeReferences();

            for (String entry: references) {
               Geometry geometry = nodesWithIDs.get(entry);               //get the geometry of the node with ID=entry
               wayTmp.addNodeGeometry(geometry);                          //add the node geometry in this way
               
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
            
            wayList.add(wayTmp);
            waysWithIDs.put(wayTmp.getID(), wayTmp.getGeometry());                    //Keep a dictionary of all way geometries, just in case they might be needed to handle OSM relations
        } 
        
        if(element.equalsIgnoreCase("relation")) {
            relationList.add(relationTmp);
        }
    }

    public List<OSMNode> getNodeList() {
        return nodeList;
    }

    public List<OSMWay> getWayList() {
        return wayList;
    }
    
    public List<OSMRelation> getRelationList(){
        return relationList;
    }

    public Map<String, Geometry> getNodesWithIDs() {
        return nodesWithIDs;
    }    
    
    public Map<String, Geometry> getWaysWithIDs() {
        return waysWithIDs;
    } 
}


