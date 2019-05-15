/*
 * @(#) CsvToRdf.java 	 version 1.8   5/10/2018
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
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;

import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Converter;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.GraphConverter;
import eu.slipo.athenarc.triplegeo.utils.RMLConverter;
import eu.slipo.athenarc.triplegeo.utils.StreamConverter;


/**
 * Main entry point of the utility for extracting RDF triples from CSV file.
 * Instead of just lon/lat attributes for points, this utility also supports more complex geometry types, provided that input CSV includes an attribute with the WKT representation of such geometries.
 * LIMITATIONS: Currently, only supporting CSV files with header (i.e., named attributes).
 *              Apart from a delimiter, configuration files for CSV records must also specify whether there is a quote character in string values.
 * @author Kostas Patroumpas
 * @version 1.8
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 16/2/2017
 * Modified: 16/2/2017, added support for transformation from a given CRS to WGS84
 * Modified: 22/2/2017, added support for exporting custom geometries to (1) Virtuoso RDF and (2) according to WGS84 Geopositioning RDF vocabulary 
 * Modified: 19/4/2017, added support for UTF-8 character strings in attribute values
 * Modified: 4/9/2017, tested support for transformation according to RML mappings
 * Modified: 3/11/2017, added support for system exit codes on abnormal termination
 * Modified: 7/11/2017, fixed issue with multiple instances of CRS factory
 * Modified: 24/11/2017, added support for recognizing character encoding for strings
 * Modified: 12/12/2017, fixed issue with string encodings; verified that UTF characters read and written correctly
 * Last modified by: Kostas Patroumpas, 5/10/2018
 */
public class CsvToRdf {

	  Converter myConverter;
	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  int sourceSRID;                         //Source CRS according to EPSG 
	  int targetSRID;                         //Target CRS according to EPSG
	  private Configuration currentConfig;    //User-specified configuration settings
	  private Classification classification;  //Classification hierarchy for assigning categories to features
	  private String inputFile;               //Input CSV file
	  private String outputFile;              //Output RDF file
	  private String encoding;                //Encoding of the data records
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
	  
	  private String[] csvHeader = null;    //CSV Header

	   
	  /**
	   * Constructor for the transformation process from CSV file to RDF.
	   * @param config  Parameters to configure the transformation.
	   * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	   * @param inFile   Path to input CSV file.
	   * @param outFile  Path to the output file that collects RDF triples.
	   * @param sourceSRID  Spatial reference system (EPSG code) of the input geometries.
	   * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	   */
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
	 * Loads the CSV file from the configuration path and returns an iterable feature collection.  
	 * @param filePath  The path to the CSV file containing data features.
	 * @return  Iterator over the collection of CSV records that can be streamed into the transformation process.
	 */
	@SuppressWarnings("resource")
	private Iterator<CSVRecord> collectData(String filePath) {
	  
		Iterator<CSVRecord> records = null;
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
			records = dataCSVParser.iterator();                                  //List of all records
			Map<String, Integer> headerMap = dataCSVParser.getHeaderMap();     
			csvHeader = headerMap.keySet().toArray(new String[headerMap.size()]);  //Array of all column names
			
		} catch (Exception e) {
  			ExceptionHandler.abort(e, "Cannot access input file.");      //Execution terminated abnormally
  		}
		
		return records;
	}
	 
	
  /**
    * Applies transformation according to the configuration settings.
    */  
   public void apply() {

      try {
			//Collect results from the CSV file
			Iterator<CSVRecord> rs = collectData(inputFile);
			
			if (currentConfig.mode.contains("GRAPH"))
			{
			  //Mode GRAPH: write triples into a disk-based Jena model and then serialize them into a file
			  myConverter = new GraphConverter(currentConfig, outputFile);
			
			  //Export data after constructing a model on disk
			  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
			  
			  //Remove all temporary files as soon as processing is finished
			  myAssistant.removeDirectory(myConverter.getTDBDir());
			}
			else if (currentConfig.mode.contains("STREAM"))
			{
			  //Mode STREAM: consume records and streamline them into a serialization file
			  myConverter =  new StreamConverter(currentConfig, outputFile);
			  
			  //Export data in a streaming fashion
			  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
			}
			else if (currentConfig.mode.contains("RML"))
			{
			  //Mode RML: consume records and apply RML mappings in order to get triples
			  myConverter =  new RMLConverter(currentConfig);
			  
			  myConverter.setHeader(csvHeader);
			  
			  //Export data in a streaming fashion according to RML mappings
			  myConverter.parse(myAssistant, rs, classification, reproject, targetSRID, outputFile);
			}
      } catch (Exception e) {
    	  ExceptionHandler.abort(e, "");
  	  }     
   }

}
