/*
 * @(#) GpxToRdf.java 	 version 1.9   12/7/2019
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.xerces.dom.ElementNSImpl;
import org.w3c.dom.NodeList;

import com.topografix.gpx.GpxType;
import com.topografix.gpx.TrkType;
import com.topografix.gpx.TrksegType;
import com.topografix.gpx.WptType;


import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from GPX files.
 * LIMITATIONS: Currently supporting WAYPOINT and TRACK features only!
 * GPX manual available at: http://www.topografix.com/gpx_manual.asp
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 1/3/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 19/12/2017, reorganized collection of triples using TripleGenerator
 * Modified: 24/1/2018, included auto-generation of UUIDs for the URIs of features
 * Modified: 7/2/2018, added support for exporting all available non-spatial attributes as properties
 * Modified: 9/5/2018, unified handling for GRAPH and STREAM transformation modes
 * Modified: 16/5/2018; handling of extra user-specified attributes in GPX extensions
 * Modified: 25/10/2018; integrate handling of a user-specified classification scheme for features.
 * Last modified by: Kostas Patroumpas, 12/7/2019
*/


public class GpxToRdf {

	Converter myConverter;
	Assistant myAssistant;
	int sourceSRID;                        //Source CRS according to EPSG 
	int targetSRID;                        //Target CRS according to EPSG
	private Configuration currentConfig;   //User-specified configuration settings
	private Classification classification; //Classification hierarchy for assigning categories to features
	private String inputFile;              //Input GPX file
	private String outputFile;             //Output RDF file

	/**
	 * Constructor for the transformation process from GPX file to RDF.
	 * @param config  Parameters to configure the transformation.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param inFile  Path to input GPX file.
	 * @param outFile  Path to the output file that collects RDF triples.
	 * @param sourceSRID  Spatial reference system (EPSG code) of the input GPX file.
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @throws ClassNotFoundException
	 */
	public GpxToRdf(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  currentConfig = config;
		  this.classification = classific;
		  inputFile = inFile;
		  outputFile = outFile;
		  this.sourceSRID = 4326;            //ASSUMPTION: All geometries in GPX are always expected in WGS84 georeference
	      this.targetSRID = 4326;
	      myAssistant = new Assistant(config);
	      
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
	    	    //Collect features from the GPX file
				GpxType gpx = collectData(inputFile);
				
				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig, myAssistant, outputFile);
				  			
				  //Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
				  parseDocument(gpx);
				  
				  //Export the RDF graph into a user-specified serialization
				  myConverter.store(outputFile);
				  
				  //Remove all temporary files as soon as processing is finished
				  myAssistant.removeDirectory(myConverter.getTDBDir());
				}
				else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig, myAssistant, outputFile);
				
				  //Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
				  parseDocument(gpx);
				  
				  //Finalize the output RDF file
				  myConverter.store(outputFile);
				}
				else   //TODO: Implement method for handling transformation using RML mappings
				{
					System.err.println("Transformation of GPX data is possible under either GRAPH or STREAM mode. RML mode is currently not supported.");
					throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
				}
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "");
	  	  }

	}
	  
	
	/**
	 * Loads the GPX file from the configuration path and returns the GPX features (waypoints, tracks) identified according to the GPX schema. 
	 * @param filePath   The path to the GPX file.
	 * @return  Instantiation of a GPX document containing all identified waypoints and tracks.
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private static GpxType collectData(String filePath) throws IOException {
		  
			JAXBContext jc = null;
		    GpxType gpx = null;
		    try {
		        jc = JAXBContext.newInstance("com.topografix.gpx");      //Relate to the XSD schema for GPX features
		        Unmarshaller unmarshaller = jc.createUnmarshaller();
		        JAXBElement<GpxType> root = (JAXBElement<GpxType>)unmarshaller
		            .unmarshal(new File(filePath)); 
		        gpx = root.getValue();
		    } catch(JAXBException ex) {
		       ex.printStackTrace();
		    }
			
			return gpx; 
	}
	  
	  
	
	/**
	 * Parse each item (waypoint, track) from GPX document in order to create the necessary triples on disk (including geometric and non-spatial attributes).  
	 * @param gpx  The GPX document with waypoints and tracks.
	 */
	private void parseDocument(GpxType gpx) {
	    	    
	    try {
		    //PHASE I: Iterate through all tracks in the GPX file
		    List<TrkType> tracks = gpx.getTrk();
		    for (TrkType track : tracks) 
		    {    					
		    	//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
				//String uuid = myAssistant.getUUID(currentConfig.featureSource, track.toString()).toString();

		        List<TrksegType> trackSegments = track.getTrkseg();
	
		        //Gather all non-spatial attributes into a temporary map
				Map<String, String> row = new HashMap<String, String>();
				addTagValue(row, "name", track.getName());
				addTagValue(row, "cmt", track.getCmt());
				addTagValue(row, "desc", track.getDesc());
				addTagValue(row, "src", track.getSrc());
				addTagValue(row, "link", track.getLink());
				addTagValue(row, "number", track.getNumber());
				addTagValue(row, "type", track.getType());

				//Handle extensions by adding each one as an extra attribute into the temporary map
				addExtensionValues(row, track.getExtensions().getAny());
				
				//Geometry may be a LINESTRING (for a single track segment) or a MULTILINESTRING (if track consists of multiple segments)
				String wkt;
				Boolean singleSegment = (trackSegments.size() == 1);
				if (singleSegment)
					wkt = "LINESTRING ";
				else
					wkt = "MULTILINESTRING (";
		
	            //Extract geometry for each track segment
		        for (TrksegType seg: trackSegments) {		        	
		        	wkt  = wkt + "(";
		            List<WptType> points = seg.getTrkpt();
		            for(WptType point : points) {
		                //System.out.println("TrackPoint recorded at " + point.getTime() + " at elevation: " + point.getEle() + " with coordinates: " + point.getLon() + "," + point.getLat());
		            	wkt  = wkt + point.getLon() + " " + point.getLat() + ","; 	
		            }
		            wkt = wkt.substring(0, wkt.length()-1) + ")";	  
		            
		            if (!singleSegment)
		            	wkt = wkt + ",";     //Separator of segments in MULTILINESTRING
		        }  
		        
	            if (!singleSegment)
	            	wkt = wkt + ")";         //Close MULTILINESTRING    
			 
				//Process all available attributes (including geometry)
				//CAUTION! Currently, each non-spatial attribute name is used as the property in the resulting triple
				if (!row.isEmpty())
					myConverter.parse(wkt, row, classification, targetSRID, "LINESTRING");
		    }	    
		    
		    //PHASE II: Iterate through all waypoints in the GPX file
		    List<WptType> points = gpx.getWpt();
	
		    for (WptType point : points) 
		    {
	//		    System.out.println("WayPoint: " + point.getName() + " of type: " + point.getType() + " with comments: " + point.getCmt() + " with coordinates: " + point.getLon() + "," + point.getLat());
		    	
		    	//Waypoint coordinates are always assumed to be georeferenced in WGS84 
				String wkt = "POINT (" + point.getLon() + " " + point.getLat() + ")";
	
				//CAUTION! On-the-fly generation of a UUID for this feature, giving as seed the data source and the identifier of that feature
				//String uuid = myAssistant.getUUID(currentConfig.featureSource, point.toString()).toString();
	  	        
				//Gather all non-spatial attributes into a temporary map
				Map<String, String> row = new HashMap<String, String>();
				addTagValue(row, "ele", point.getEle());
				addTagValue(row, "time", point.getTime());
				addTagValue(row, "magvar", point.getMagvar()); 
				addTagValue(row, "geoidheight", point.getGeoidheight());
				addTagValue(row, "name", point.getName());
				addTagValue(row, "cmt", point.getCmt());
				addTagValue(row, "desc", point.getDesc());
				addTagValue(row, "src", point.getSrc());
				addTagValue(row, "link", point.getLink());
				addTagValue(row, "sym", point.getSym());
				addTagValue(row, "type", point.getType());
				addTagValue(row, "fix", point.getFix());
				addTagValue(row, "sat", point.getSat());
				addTagValue(row, "hdop", point.getHdop());
				addTagValue(row, "vdop", point.getVdop());
				addTagValue(row, "pdop", point.getPdop());
				addTagValue(row, "ageofdgpsdata", point.getAgeofdgpsdata());
				addTagValue(row, "dgpsid", point.getDgpsid());
			    		
				//Handle extensions by adding each one as an extra attribute into the temporary map
				addExtensionValues(row, point.getExtensions().getAny());
		
				//Process all available attributes (including geometry)
				//CAUTION! Currently, each non-spatial attribute name is used as the property in the resulting triple
				if (!row.isEmpty())
					myConverter.parse(wkt, row, classification, targetSRID, "POINT");
				
		    }
		}
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
	
	}	  

	
	/**
	 * Updates a dictionary with the given value for an attribute (its original name is used as tag)
	 * @param row  Dictionary with (tag, value) pairs
	 * @param tag  Key to be found/added in the dictionary
	 * @param val  Value to be updated/added into the dictionary
	 */
	@SuppressWarnings("rawtypes")
	private void addTagValue(Map<String,String> row, String tag, Object val) {
		  
	  //Ignore attributes carrying an empty list
	  if ((val instanceof List) && ((List)val).isEmpty())
		  return;
		  
	  if (val != null)
		  row.put(tag,  val.toString().trim());
	}

	
	/**
	 * Updates a dictionary with the values specified in custom GPX extensions (the name of the custom attribute is used as the key for the respective value)
	 * @param row  Dictionary with (tag, value) pairs
	 * @param ext  The list of extensions specified for a GPX waypoint or track
	 */
	private void addExtensionValues(Map<String,String> row, List<Object> ext) {

		for (Object e: ext)
		{
			NodeList nl = ((ElementNSImpl) e).getChildNodes();
			for (int i =0; i< nl.getLength(); i++) {
				if (nl.item(i).getLocalName() != null)                 //Ignore null values in attributes specified in this extension
					addTagValue(row, nl.item(i).getLocalName(), nl.item(i).getTextContent().trim());
			}
		}
	}
	
}
