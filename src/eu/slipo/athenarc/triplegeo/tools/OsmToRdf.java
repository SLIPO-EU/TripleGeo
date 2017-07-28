/*
 * @(#) OsmToRdf.java	version 1.2   28/7/2017
 *
 * Copyright (C) 2013-2017 Institute for the Management of Information Systems, Athena RC, Greece.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import eu.slipo.athenarc.triplegeo.osm.OSMNode;
import eu.slipo.athenarc.triplegeo.osm.OSMRelation;
import eu.slipo.athenarc.triplegeo.osm.OSMWay;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;
import eu.slipo.athenarc.triplegeo.osm.OsmParser;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Entry point to convert OpenStreetMap (OSM) XML files into RDF triples.
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 19/4/2017
 * Last modified by: Kostas Patroumpas, 28/7/2017
 * CAUTION: Current limitation in the transformation process: due to in-memory indexing of OSM features, this process can handle only a moderate amount of OSM features. 
 */
public class OsmToRdf {

	  Converter myConverter;
	  public int sourceSRID;                //Source CRS according to EPSG 
	  public int targetSRID;                //Target CRS according to EPSG
	  private Configuration currentConfig;  //User-specified configuration settings
	  private String inputFile;             //Input OSM file
	  private String outputFile;            //Output RDF file
	
		
	  //Class constructor
	  public OsmToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
       
		  currentConfig = config;
		  inputFile = inFile;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;      //Currently assuming that both OSM input and RDF output will be georeferenced in WGS84
	      this.targetSRID = targetSRID;      //..., so no transformation is needed
	      
	      // Other parameters
	      if (Assistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }
	  }

	  
	/*
	 * Apply transformation according to configuration settings.
	 */  
	public void apply() {

	      try {
				//Collect results from the CSV file
				Iterable<OSMRecord> rs = collectData(inputFile);
				
				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig);
				
				  //Export data after constructing a model on disk
				  executeParser4Graph(rs);
				  
				  //Remove all temporary files as soon as processing is finished
				  Assistant.removeDirectory(currentConfig.tmpDir);
				}
				else					
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4Stream(rs);
				}
	      } catch (Exception e) {
	  			e.printStackTrace();
	  	  }

	}
	

  /**
   * Parses the OSM XML file from the configuration path and returns the
   * feature collection associated according to the configuration.
   *
   * @param osmFilePath with the path to the OSM XML file.
   *
   * @return FeatureCollection with the collection of features filtered.
   */
  private static Iterable<OSMRecord> collectData(String osmFilePath) throws IOException
  {		  
	  List<OSMRecord> listRecs = new ArrayList<OSMRecord>();
			
		List<OSMNode> nodeList;
		List<OSMWay> wayList;
		List<OSMRelation> relationList;
		Map<String, Geometry> nodesWithIDs;
		Map<String, Geometry> waysWithIDs;
		
	    OsmParser osmParser = null;
	    try {
	        System.out.println("Accessing file...");
	        osmParser = new OsmParser(osmFilePath);
	        System.out.println("OSM file accessed!");
	    } catch (Exception ex) {
	        Logger.getLogger(OsmToRdf.class.getName()).log(Level.SEVERE, null, ex);
	    }
	    
	    //Parse the XML file    
	    osmParser.parseDocument();
	    
	    nodeList = osmParser.getNodeList();
	    relationList = osmParser.getRelationList();
	    wayList = osmParser.getWayList();
	    nodesWithIDs = osmParser.getNodesWithIDs();
	    waysWithIDs = osmParser.getWaysWithIDs();

//	    System.out.println("***********************************NODES***********************************");
	    
	    for (OSMNode n: nodeList) {
	    	if (n.getTagKeyValue().containsKey("name"))
	    	{
	    		OSMRecord rec = new OSMRecord();
		    	rec.setID("N" + n.getID());
		    	rec.setType(n.getTagKeyValue().get("type"));
		    	rec.setName(n.getTagKeyValue().get("name"));
		    	rec.setGeometry(n.getGeometry());
		    	rec.setTags(n.getTagKeyValue());
		    	listRecs.add(rec);
		    }
	    }

//	    System.out.println("***********************************WAYS***********************************");
	    
	    for (OSMWay w: wayList) {
	    	if (w.getTagKeyValue().containsKey("name"))
	    	{
	    		OSMRecord rec = new OSMRecord();
		    	rec.setID("W" + w.getID());
		    	rec.setType(w.getTagKeyValue().get("type"));
		    	rec.setName(w.getTagKeyValue().get("name"));
		    	rec.setGeometry(w.getGeometry());
		    	rec.setTags(w.getTagKeyValue());
		    	listRecs.add(rec);
	    	}
	    }
	    
//	    System.out.println("***********************************RELATIONS***********************************");
	    
	    for (OSMRelation r: relationList) {
	    	if (r.getTagKeyValue().containsKey("name"))
	    	{
	    		OSMRecord rec = new OSMRecord();
	    		GeometryCollection geomCollection = null;
		    	rec.setID("R" + r.getID());
		    	rec.setType(r.getTagKeyValue().get("type"));
		    	rec.setName(r.getTagKeyValue().get("name"));
		    	rec.setTags(r.getTagKeyValue());
		    	
		    	//Reconstruct geometry from relating with its elementary nodes and ways
		    	GeometryFactory geometryFactory = new GeometryFactory();
		    	Geometry[] memberGeometries = new Geometry[r.getMemberReferences().size()];
		    	int numMembers = 0;
		    	  
		    	if ((r.getTagKeyValue().get("type") != null) && ((r.getTagKeyValue().get("type").equalsIgnoreCase("route")) || (r.getTagKeyValue().get("type").equalsIgnoreCase("multilinestring")) || (r.getTagKeyValue().get("type").equalsIgnoreCase("multipolygon")) || (r.getTagKeyValue().get("type").equalsIgnoreCase("boundary"))))
		    	{
		    		for (String member: r.getMemberReferences())       //Handle ways
		    		{	
		    			if (waysWithIDs.get(member) != null)
		    			{
		    				if (waysWithIDs.get(member) != null)
		    				{
			    				memberGeometries[numMembers] = waysWithIDs.get(member);     //Reference to OSMWay geometry
			    				numMembers++;
		    				}
		    			}
		    			else if (nodesWithIDs.get(member) != null)     //Handle nodes
		    			{
		    				if (nodesWithIDs.get(member) != null)
		    				{
			    				memberGeometries[numMembers] = nodesWithIDs.get(member);    //Reference to OSMNode geometry
			    				numMembers++;
		    				}
		    			}
		    		}
		    		
		    		if (numMembers > 0)
		    		{
		    			geomCollection = geometryFactory.createGeometryCollection(java.util.Arrays.copyOfRange(memberGeometries, 0, numMembers-1));
//				    	System.out.print(geomCollection.toString());
		    		}
		    	}
		    	
		    	rec.setGeometry(geomCollection);
		    	listRecs.add(rec);
	    	}
	    }
	     
	  System.out.println("Finished! " + listRecs.size() + " OSM features (nodes, ways, relations) have been parsed.");   	
	  Iterable<OSMRecord> records = listRecs;
	  return records;
  }
	
  /**
   * 
   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
   */
  private void executeParser4Graph(Iterable<OSMRecord> records) {

    //System.out.println(Assistant.getGMTime() + " Started processing features...");
    long t_start = System.currentTimeMillis();
    long dt = 0;
   
    int numRec = 0;
    
//  System.out.println("INPUT: " + UtilsLib.currentConfig.inputFile + " DELIMITER:***"+ UtilsLib.currentConfig.delimiter +"***");
  
	try {   
		//Iterate through all records
		for (OSMRecord rs : records) {
			
			String wkt = rs.getGeometry().toText();   //Get WKT representation
	      	
			//Parse geometric representation
	      	myConverter.parseGeom2RDF(currentConfig.featureName, rs.getID(), wkt, targetSRID);
	      	
			//Process non-spatial attributes for name and type
			if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
				myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.getID(), rs.getName(), rs.getType());		

			Assistant.notifyProgress(++numRec);
			
		}
    }
	catch(Exception e) { }

    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    System.out.println(Assistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
    System.out.println(Assistant.getGMTime() + " Starting to write triples to file...");
    
    //Count the number of statements
    int numStmt = myConverter.getModel().listStatements().toSet().size();
    
    //Export model to a suitable format
    try {
    	FileOutputStream out = new FileOutputStream(outputFile);
    	myConverter.getModel().write(out, currentConfig.serialization);  
    }
    catch(Exception e) { System.out.println("Serialized output cannot be written into a file. Please check configuration file.");}
    
    //Final notification
    dt = System.currentTimeMillis() - t_start;
    Assistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, outputFile);
  }

  
  /**
   * 
   * Mode: STREAM -> Parse each OSM entity and streamline the resulting triples (including geometric and non-spatial attributes)
   */
  private void executeParser4Stream(Iterable<OSMRecord> records) {

    //System.out.println(Assistant.getGMTime() + " Started processing features...");
    long t_start = System.currentTimeMillis();
    long dt = 0;
   
    int numRec = 0;
    int numTriples = 0;
	    
	  OutputStream outFile = null;
	  try {
			outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
	  } 
	  catch (FileNotFoundException e) {
			e.printStackTrace();
	  } 
		
	  //CAUTION! Hard constraint: serialization into N-TRIPLES is only supported by Jena riot (stream) interface
	  StreamRDF stream = StreamRDFWriter.getWriterStream(outFile, RDFFormat.NTRIPLES);   //StreamRDFWriter.getWriterStream(os, Lang.NT);
		
	  stream.start();             //Start issuing streaming triples
    
	try {
		//Iterate through all records
		for (OSMRecord rs : records) {
	
			myConverter =  new StreamConverter(currentConfig);
			
			String wkt = rs.getGeometry().toText();    //Get WKT representation
	      	
			//Parse geometric representation
			myConverter.parseGeom2RDF(currentConfig.featureName, rs.getID(), wkt, targetSRID);	 
			
			//Process non-spatial attributes for name and type
			if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
				myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.getID(), rs.getName(), rs.getType());	
			
			//Append each triple to the output stream 
			for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
				stream.triple(myConverter.getTriples().get(i));
				numTriples++;
			}
			
			Assistant.notifyProgress(++numRec);
			
		}
    }
	catch(Exception e) { e.printStackTrace();}

	stream.finish();               //Finished issuing triples
	
    //Measure execution time
    dt = System.currentTimeMillis() - t_start;
    Assistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
  }

}
