/*
 * @(#) CsvToRdf.java 	 version 1.3   28/11/2017
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import be.ugent.mmlab.rml.model.dataset.SimpleRMLDataset;
//import be.ugent.mmlab.rml.model.dataset.StdRMLDataset;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from CSV file 
 * CAUTION: Currently, only supporting CSV files with header (i.e., named attributes)
 * CAUTION: Apart from a delimiter, configuration files for CSV records must laso specify whether there is a quote character in string values
 * IMPORTANT: Currently, only 2-dimensional POINT geometries can be extracted
 * FIXME: Verify that UTF characters are read and written correctly!
 * Created by: Kostas Patroumpas, 16/2/2017
 * Modified: 16/2/2017, added support for transformation from a given CRS to WGS84
 * Modified: 22/2/2017, added support for exporting custom geometries to (1) Virtuoso RDF and (2) according to WGS84 Geopositioning RDF vocabulary 
 * Modified: 19/4/2017, added support for UTF-8 character strings in attribute values
 * Modified: 4/9/2017, tested support for transformation according to RML mappings
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 7/11/2017, fixed issue with multiple instances of CRS factory
 * Modified: 24/11/2017, added support for recognizing character encoding for strings
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */
public class CsvToRdf {

	  Converter myConverter;
	  Assistant myAssistant;
	  private MathTransform transform = null;
	  private WKTReader reader = null;
	  public int sourceSRID;                 //Source CRS according to EPSG 
	  public int targetSRID;                 //Target CRS according to EPSG
	  private Configuration currentConfig;   //User-specified configuration settings
	  private Classification classification; //Classification hierarchy for assigning categories to features
	  private String inputFile;              //Input CSV file
	  private String outputFile;             //Output RDF file
	  private String encoding;               //Encoding of the data records
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	  private String[] csvHeader = null;    //CSV Header
		
	  //Class constructor
	  public CsvToRdf(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) {
       
		  myAssistant = new Assistant();
		  currentConfig = config;
		  classification = classific;
		  inputFile = inFile;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      encoding = config.encoding;   //User-specified encoding
		  
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.sourceCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
	  	        
	  	        //Needed for parsing original geometry in WTK representation
	  	        GeometryFactory geomFactory = new GeometryFactory(new PrecisionModel(), sourceSRID);
	  	        reader = new WKTReader(geomFactory);
	  	        
	  		} catch (Exception e) {
	  			ExceptionHandler.invoke(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
	  		}
	      
	      // Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
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
	@SuppressWarnings("resource")
	private Iterable<CSVRecord> collectData(String filePath) {
		  
			Iterable<CSVRecord> records = null;
			try {
				File file = new File(filePath);
				
				//Check for several UTF encodings and change the default one, if necessary
				BOMInputStream bomIn = new BOMInputStream(new FileInputStream(file), ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE);
				if (bomIn.hasBOM(ByteOrderMark.UTF_8)) 
					encoding = StandardCharsets.UTF_8.name();
				else if (bomIn.hasBOM(ByteOrderMark.UTF_16LE)) 
					encoding = StandardCharsets.UTF_16LE.name();
				else if (bomIn.hasBOM(ByteOrderMark.UTF_16BE))
					encoding = StandardCharsets.UTF_16BE.name();
				
				//Read records and header from the file
				Reader in = new InputStreamReader(new FileInputStream(file), encoding);
				CSVFormat format = CSVFormat.RFC4180.withDelimiter(currentConfig.delimiter).withQuote(currentConfig.quote).withFirstRecordAsHeader();	
				CSVParser dataCSVParser = new CSVParser(in, format);
				records = dataCSVParser.getRecords();                                  //List of all records
				Map<String, Integer> headerMap = dataCSVParser.getHeaderMap();     
				csvHeader = headerMap.keySet().toArray(new String[headerMap.size()]);  //Array of all column names
				
			} catch (Exception e) {
	  			ExceptionHandler.invoke(e, "Cannot access input file.");      //Execution terminated abnormally
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
				  myAssistant.removeDirectory(currentConfig.tmpDir);
				}
				else if (currentConfig.mode.contains("STREAM"))
				{
				  //Mode STREAM: consume records and streamline them into a serialization file
				  myConverter =  new StreamConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4Stream(rs);
				}
				else
				{
				  //Mode RML: consume records and apply RML mappings in order to get triples
				  myConverter =  new RMLConverter(currentConfig);
				  
				  //Export data in a streaming fashion
				  executeParser4RML(rs);
				}
	      } catch (Exception e) {
	    	  ExceptionHandler.invoke(e, "");
	  	  }

	}
	  /**
	   * 
	   * Mode: GRAPH -> Parse each record in order to create the necessary triples on disk (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Graph(Iterable<CSVRecord> records) {

	    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
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
		      		wkt = myAssistant.wktTransform(wkt, transform, reader);     //Get transformed WKT representation
		      	
				//Parse geometric representation
		      	myConverter.parseGeom2RDF(currentConfig.featureName, rs.get(currentConfig.attrKey), wkt, targetSRID);
	//System.out.print("Geometry converted!");	
		      	
				//Process non-spatial attributes for name and type
				if ((!currentConfig.attrCategory.trim().equals("")) && (!currentConfig.attrName.trim().equals("")))
					myConverter.handleNonGeometricAttributes(currentConfig.featureName, rs.get(currentConfig.attrKey), rs.get(currentConfig.attrName), rs.get(currentConfig.attrCategory));
	//System.out.print("Non-Geometric attributes converted!");			

				myAssistant.notifyProgress(++numRec);
				
			}
	    }
		catch(Exception e) { 
			ExceptionHandler.invoke(e, "An error occurred during transformation of an input record.");
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    System.out.println(myAssistant.getGMTime() + " Parsing completed for " + numRec + " records in " + dt + " ms.");
	    System.out.println(myAssistant.getGMTime() + " Starting to write triples to file...");
	    
	    //Count the number of statements
	    int numStmt = myConverter.getModel().listStatements().toSet().size();
	    
	    //Export model to a suitable format
	    try {
	    	FileOutputStream out = new FileOutputStream(outputFile);
	    	myConverter.getModel().write(out, currentConfig.serialization);  
	    }
	    catch(Exception e) { 
	    	ExceptionHandler.invoke(e, "Serialized output cannot be written into a file. Please check configuration file.");
	    }
	    
	    //Final notification
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numStmt, currentConfig.serialization, outputFile);
	  }

	  
	  /**
	   * 
	   * Mode: STREAM -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes)
	   */
	  private void executeParser4Stream(Iterable<CSVRecord> records) {

	    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	   
	    int numRec = 0;
	    int numTriples = 0;
		    
		  OutputStream outFile = null;
		  try {
				outFile = new FileOutputStream(outputFile);   //new ByteArrayOutputStream();
		  } 
		  catch (FileNotFoundException e) {
			  ExceptionHandler.invoke(e, "Output file not specified correctly.");
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
		      		wkt = myAssistant.wktTransform(wkt, transform, reader);     //Get transformed WKT representation
		      	
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
				
				myAssistant.notifyProgress(++numRec);
				
			}
	    }
		catch(Exception e) { 
			ExceptionHandler.invoke(e, "An error occurred during transformation of an input record.");
		}

		stream.finish();               //Finished issuing triples
		
	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
	  }



	  
	  /**
	   * 
	   * Mode: RML -> Parse each record and streamline the resulting triples (including geometric and non-spatial attributes) according to the given RML mapping
	   */
	  private void executeParser4RML(Iterable<CSVRecord> records) {

	    //System.out.println(myAssistant.getGMTime() + " Started processing features...");
	    long t_start = System.currentTimeMillis();
	    long dt = 0;
	   
	    int numRec = 0;
	    int numTriples = 0;

//	    RMLDataset dataset = new StdRMLDataset();
	    RMLDataset dataset = new SimpleRMLDataset();
	    
	    //Determine the serialization to be applied for the output triples
	    org.openrdf.rio.RDFFormat rdfFormat =  myAssistant.getRDFSerialization(currentConfig.serialization);
	    	  
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
			
			//Iterate through all records
			for (CSVRecord rs : records) 
			{
		      	//Pass all attribute values into a hash map in order to apply RML mapping(s) directly
		      	HashMap<String, String> row = new HashMap<>();	
		      	for (int i=0; i<rs.size(); i++)
		      	{
		      	    //Names of attributes in upper case; case-sensitive in RML mappings!
		      		row.put(csvHeader[i].toUpperCase(), myAssistant.encodeUTF(rs.get(i), encoding));
		      	}
		      	
		      	//Include geometry attribute, if specified
		      	if ((currentConfig.attrX != null) && (currentConfig.attrY != null))
		      	{
				    String x = rs.get(currentConfig.attrX);
				    String y = rs.get(currentConfig.attrY);
					String wkt = "POINT (" + x + " " + y + ")";
//					System.out.println(wkt);
					
					//CRS transformation
			      	if (transform != null)
			      		wkt = myAssistant.wktTransform(wkt, transform, reader);     //Get transformed WKT representation

		      		row.put("WKT", "<http://www.opengis.net/def/crs/EPSG/0/" + targetSRID + "> " + wkt);   //Extra attribute for the geometry as WKT
		      	}
		      
		      	//Include a category identifier, as found in the classification scheme
		      	if (classification != null)
		      		row.put("CATEGORY_URI", classification.getURI(rs.get(currentConfig.attrCategory)));
		      	//System.out.println(classification.getURI(rs.getString(currentConfig.attrCategory)));

		      	//Also include a UUID as a 128-bit string
		      	//row.put("UUID", myAssistant.getUUID(rs.get(currentConfig.attrKey)).toString());
		      	
		        //Apply the transformation according to the given RML mapping		      
		        myConverter.parseWithRML(row, dataset);
		       
		        myAssistant.notifyProgress(++numRec);
			  
			    //Periodically, dump results into output file
				if (numRec % 10 == 0) 
				{
					numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, "UTF-8");
					dataset = new SimpleRMLDataset();		   //IMPORTANT! Create a new dataset to hold upcoming triples!	
//					dataset = new StdRMLDataset();		       //IMPORTANT! Create a new dataset to hold upcoming triples!	
				}
			}	
			
			//Dump any pending results into output file
			numTriples += myConverter.writeTriples(dataset, writer, rdfFormat, "UTF-8");
			writer.flush();
			writer.close();
	    }
		catch(Exception e) { 
			ExceptionHandler.invoke(e, "Please check RML mappings.");
		}

	    //Measure execution time
	    dt = System.currentTimeMillis() - t_start;
	    myAssistant.reportStatistics(dt, numRec, numTriples, currentConfig.serialization, outputFile);
	  }
	  
}
