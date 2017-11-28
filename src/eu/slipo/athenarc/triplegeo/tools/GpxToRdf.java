/*
 * @(#) GpxToRdf.java 	 version 1.3   24/11/2017
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import com.topografix.gpx.GpxType;
import com.topografix.gpx.TrkType;
import com.topografix.gpx.TrksegType;
import com.topografix.gpx.WptType;


import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from GPX files (currently, WAYPOINT and TRACK features only!)
 * TODO: Implement transformation using a disk-based graph model; Handle non-geometric attributes for tracks (if applicable)
 * GPX manual available at: http://www.topografix.com/gpx_manual.asp
 * Created by: Kostas Patroumpas, 1/3/2017
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Last modified by: Kostas Patroumpas, 24/11/2017
*/


public class GpxToRdf {

	Converter myConverter;
	Assistant myAssistant;
	public int sourceSRID;                //Source CRS according to EPSG 
	public int targetSRID;                //Target CRS according to EPSG
	private Configuration currentConfig;  //User-specified configuration settings
	private String inputFile;             //Input GPX file
	private String outputFile;            //Output RDF file

	  //Class constructor
	  public GpxToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
  
		  currentConfig = config;
		  inputFile = inFile;
		  outputFile = outFile;
		  this.sourceSRID = 4326;       //ASSUMPTION: All geometries in GPX are always expected in WGS84 georeference
	      this.targetSRID = 4326;
	      myAssistant = new Assistant();
	      
	      // Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }
	      
	  }

	/*
	 * Apply transformation according to configuration settings.
	 */  
	public void apply() {
		
	      try {
	    	    //Collect features from the GPX file
				GpxType gpx = collectData(inputFile);
				
				if (currentConfig.mode.contains("GRAPH"))
				{
				  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
				  myConverter = new GraphConverter(currentConfig);
				 
				  //Export data after constructing a model on disk
				  executeParser4Graph(gpx);
				  
				  //Remove all temporary files as soon as processing is finished
				  myAssistant.removeDirectory(currentConfig.tmpDir);
				}
				else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4Stream(gpx);
				}
	      } catch (Exception e) {
	    	  ExceptionHandler.invoke(e, "");
	  	  }

	}
	  
	  /**
	   * Loads the GPX file from the configuration path and returns
	   * the GPX features (waypoints, tracks) identified according to the GPX schema.
	   *
	   * @param filePath with the path to the GPX file.
	   *
	   * @return a handle to the collection of GPX features (waypoints, tracks) identified.
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
	   * 
	   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Graph(GpxType gpx) {
		  //TODO: Implement method for disk-based model.
	  }
	  
	  
	  
	  /**
	   * 
	   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Stream(GpxType gpx) {
		
		    long t_start = System.currentTimeMillis();
		    long dt = 0;
		    int numRec = 0;
		    int numTriples = 0;
		    String id = null;
		    
			OutputStream outFile = null;
			try {
				outFile = new FileOutputStream(outputFile);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} 
			
			//IMPORTANT: Currently, it seems that only NTRIPLES serialization can be dealt with in a streaming fashion
			StreamRDF stream = StreamRDFWriter.getWriterStream(outFile, RDFFormat.NTRIPLES);
		      
		    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
		    
		    stream.start();             //Start issuing triples
		    	    
		    try {
		    	
	        //PHASE I: Iterate through all tracks in the GPX file
		    List<TrkType> tracks = gpx.getTrk();
		    for(TrkType track : tracks) {
		    	
		    	myConverter =  new StreamConverter(currentConfig);    //A new instance per track, in order to avoid maintenance
		    	
				//If a unique identifier has been specified for each feature, then use it (practically its name)
				if (currentConfig.attrKey != null)
					id = track.getName();
				else
					id = Integer.toString(numRec);               //Otherwise, resort to a serial integer				
		        //System.out.println("Track: " + track.getName());
				
		        List<TrksegType> trackSegments = track.getTrkseg();
		        for (TrksegType seg: trackSegments) {
		        	String wkt = "LINESTRING (";
		            List<WptType> points = seg.getTrkpt();
		            for(WptType point : points) {
		                //System.out.println("TrackPoint recorded at " + point.getTime() + " at elevation: " + point.getEle() + " with coordinates: " + point.getLon() + "," + point.getLat());
		            	wkt  = wkt + point.getLon() + " " + point.getLat() + ","; 	
		            }
		            wkt = wkt.substring(0, wkt.length()-1) + ")";
		            
					//Parse geometric representation
					myConverter.parseGeom2RDF(currentConfig.featureName, id, wkt, targetSRID);
					
					//TODO: Handle non-geometric attributes as well...
					
					//Append each triple to the output stream 
					for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
						stream.triple(myConverter.getTriples().get(i));
						numTriples++;
					}
					
					myAssistant.notifyProgress(++numRec);  			    
		        }    		    
		    }


		    //PHASE II: Iterate through all waypoints in the GPX file
		    List<WptType> points = gpx.getWpt();
		    for(WptType point : points) {
//		        System.out.println("WayPoint: " + point.getName() + " of type: " + point.getType() + " with comments: " + point.getCmt() + " with coordinates: " + point.getLon() + "," + point.getLat());
		    
		    	myConverter =  new StreamConverter(currentConfig);    //A new instance per waypoint, in order to avoid maintenance
		    	
		    	//Waypoint coordinates are always assumed to be georeferenced in WGS84 
				String wkt = "POINT (" + point.getLon() + " " + point.getLat() + ")";

				//If a unique identifier has been specified for each feature, then use it (practically its name)
				if (currentConfig.attrKey != null)
					id = point.getName();
				else
					id = Integer.toString(numRec);               //Otherwise, resort to a serial integer				
//				System.out.println(wkt);
		
				//Parse geometric representation
				myConverter.parseGeom2RDF(currentConfig.featureName, id, wkt, targetSRID);	 

				//Process non-spatial attributes for description (or symbol or comment) and type
				myConverter.handleNonGeometricAttributes(currentConfig.featureName, id, (currentConfig.attrName.trim().toLowerCase() == "desc" ? point.getDesc() : (point.getSym() != null ? point.getSym() : point.getCmt() )), point.getType());
		
				//Append each triple to the output stream 
				for (int i = 0; i <= myConverter.getTriples().size()-1; i++) {
					stream.triple(myConverter.getTriples().get(i));
					numTriples++;
				}
				
				myAssistant.notifyProgress(++numRec);  
		    }
		}
		catch(Exception e) { 
			ExceptionHandler.invoke(e, "An error occurred during transformation of an input record.");
		}

		stream.finish();               //Finished issuing triples
		    
		//Final notification
		dt = System.currentTimeMillis() - t_start;
		myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
	  
	  }	  
	  

}
