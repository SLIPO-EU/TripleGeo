/*
 * @(#) CsvToRdf.java 	 version 1.2   19/4/2017
 *
 * Copyright (C) 2017 Institute for the Management of Information Systems, Athena RC, Greece.
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.InputStreamReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from CSV file 
 * CAUTION: Currently, only supporting CSV files with header (i.e., named attributes)
 * IMPORTANT: Currently, only 2-dimensional POINT geometries can be extracted
 * Created by: Kostas Patroumpas, 16/2/2017
 * Modified: 16/2/2017, added support for transformation from a given CRS to WGS84
 * Modified: 22/2/2017, added support for exporting custom geometries to (1) Virtuoso RDF and (2) according to WGS84 Geopositioning RDF vocabulary 
 * Modified: 19/4/2017, added support for UTF-8 character strings in attribute values
 * Last modified by: Kostas Patroumpas, 19/4/2017
 */
public class CsvToRdf {

	  Converter myConverter;
	  private MathTransform transform = null;
	  private WKTReader reader = null;
	  public int sourceSRID;                //Source CRS according to EPSG 
	  public int targetSRID;                //Target CRS according to EPSG
	  private Configuration currentConfig;  //User-specified configuration settings
	  private String inputFile;             //Input CSV file
	  private String outputFile;            //Output RDF file
	 
		
	  //Class constructor
	  public CsvToRdf(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) throws ClassNotFoundException {
       
		  currentConfig = config;
		  inputFile = inFile;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
		  
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.sourceCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        
	  	        Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
	  	        CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
	  	        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
	  	        
	  	        //Needed for parsing original geometry in WTK representation
	  	        GeometryFactory geomFactory = new GeometryFactory(new PrecisionModel(), sourceSRID);
	  	        reader = new WKTReader(geomFactory);
	  	        
	  		} catch (Exception e) {
	  			e.printStackTrace();
	  		}
	      
	      // Other parameters
	      if (Assistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }

	  }

	  
	  /**
	   * Loads the CSV file from the configuration path and returns the
	   * feature collection associated according to the configuration.
	   *
	   * @param filePath with the path to the CSV file containing POINT data features.
	   *
	   * @return an iterator over the CSV records.
	   */
	private Iterable<CSVRecord> collectData(String filePath) throws IOException {
		  
			Iterable<CSVRecord> records = null;
			File file = new File(filePath);
			if (file.exists()) {
				Reader in = new InputStreamReader(new FileInputStream(file), "UTF-8");
				CSVFormat format = CSVFormat.RFC4180.withDelimiter(currentConfig.delimiter).withQuote('"').withFirstRecordAsHeader();
				records = format.parse(in);
			}
			return records;
	  }
	  
	/*
	 * Apply transformation according to configuration settings.
	 */  
	public void apply() {

	      try {
				//Collect results from the CSV file
				Iterable<CSVRecord> rs = collectData(inputFile);
				
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
	   * 
	   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Graph(Iterable<CSVRecord> records) {

	    //System.out.println(Assistant.getGMTime() + " Started processing features...");
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	   
	    int numRec = 0;
	    
	//  System.out.println("INPUT: " + UtilsLib.currentConfig.inputFile + " DELIMITER:***"+ UtilsLib.currentConfig.delimiter +"***");
	  
		try {   
			//Iterate through all records
			for (CSVRecord rs : records) {
				
			    String x = rs.get(currentConfig.attrX);
			    String y = rs.get(currentConfig.attrY);
				String wkt = "POINT (" + x + " " + y + ")";

				//CRS transformation
		      	if (transform != null)
		      	{
		  	      Geometry o = reader.read(wkt);
		  	      
		  	      //Attempt to transform geometry into the target CRS
		  	        try {
		  				o = JTS.transform(o, transform);
		  			} catch (Exception e) {
		  				e.printStackTrace();
		  			}
		  	         
		  	      wkt = o.toText();     //Get WKT representation
		      	}
		      	
				//Parse geometric representation
		      	myConverter.parseGeom2RDF(currentConfig.featureName, rs.get(currentConfig.attrKey), wkt, targetSRID);
	//System.out.print("Geometry converted!");	
		      	
				//Process non-spatial attributes for name and type
				if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
					myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.get(currentConfig.attrKey), rs.get(currentConfig.attrName), rs.get(currentConfig.attrCategory));
	//System.out.print("Non-Geometric attributes converted!");			

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
	   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Stream(Iterable<CSVRecord> records) {

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
	    
	//  System.out.println("INPUT: " + UtilsLib.currentConfig.inputFile + " DELIMITER:***"+ UtilsLib.currentConfig.delimiter +"***");
	  
		try {
			//Iterate through all records
			for (CSVRecord rs : records) {
		
				myConverter =  new StreamConverter(currentConfig);
				
			    String x = rs.get(currentConfig.attrX);
			    String y = rs.get(currentConfig.attrY);
				String wkt = "POINT (" + x + " " + y + ")";

//				System.out.println(wkt);
				
				//CRS transformation
		      	if (transform != null)
		      	{
		  	      Geometry o = reader.read(wkt);
		  	      
		  	      //Attempt to transform geometry into the target CRS
		  	        try {
		  				o = JTS.transform(o, transform);
		  			} catch (Exception e) {
		  				e.printStackTrace();
		  			}
		  	         
		  	      wkt = o.toText();     //Get WKT representation
		      	}
		      	
				//Parse geometric representation
				myConverter.parseGeom2RDF(currentConfig.featureName, rs.get(currentConfig.attrKey), wkt, targetSRID);	

	//System.out.println("Geometry converted!");	 
				
				//Process non-spatial attributes for name and type
				if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
					myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.get(currentConfig.attrKey), rs.get(currentConfig.attrName), rs.get(currentConfig.attrCategory));

	//System.out.println("Non-Geometric attributes converted!");		
				
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
