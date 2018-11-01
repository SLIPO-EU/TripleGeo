/*
 * @(#) ReverseConfiguration.java 	 version 1.6   2/3/2018
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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;

/**
 * Parser of user-specified configuration files for reconstructing records from an RDF graph (Reverse Transformation).
 * 
 * @author Kostas Patroumpas
 * @version 1.6
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 8/12/2017
 * Last modified: 2/3/2018
 */
public final class ReverseConfiguration {

  Assistant myAssistant;
  
  /**
   * Format of output data file. Supported formats: SHAPEFILE, CSV.
   */
  public String outputFormat;
  
  /**
   * Path to a properties file containing all parameters used in the reverse transformation.
   */
  private String path;
  
  /**
   * Path to file(s) containing input RDF dataset(s). 
   * Multiple input files (of exactly the same serialization and ontology) can be specified, separating them by ';' in order to activate multiple concurrent threads for their reverse transformation.
   */
  public String inputFiles;
  
  /**
   * Path to the output file that will contain all resulting records.
   */
  public String outputFile;
  
  /**
   * Path to a user-specified SELECT query in SPARQL that will retrieve results from the RDF graph(s).
   * This query should conform with the underlying ontology of the input RDF triples.
   */
  public String sparqlFile;
  
  /**
   * Path to a directory where intermediate files may be temporarily written during reverse transformation.
   */
  public String tmpDir;
  
  /**
   * Input RDF serialization. Supported formats: RDF/XML (default), RDF/XML-ABBREV, N-TRIPLES, TURTLE (or TTL), N3.
   */
  public String serialization;

  /**
   * Default language specification tag for string literals.
   */
  public String defaultLang = "en";
 
  /**
   * Delimiter character for attribute values in the output CSV file.
   * Mandatory for CSV output; ignored otherwise.
   */
  public char delimiter;
  
  /**
   * Quote character to be used for strings in the output CSV file.
   * Mandatory for CSV output; ignored otherwise.
   */
  public char quote;
  
  /**
   * Encoding used for strings. 
   * Default encoding: UTF-8.
   */
  public String encoding;

  /**
   * Spatial reference system (EPSG code) of geometries in the input RDF triples. If omitted, geometries are assumed in WGS84 reference system (EPSG:4326).
   * Example: EPSG:4326
   */
  public String sourceCRS;
  
  /**
   * Spatial reference system (EPSG code) of the output dataset. If omitted, the georeference of the RDF geometries will be retained (i.e., no reprojection of coordinates will take place).
   * Example: EPSG:4326
   */
  public String targetCRS;
  
  /**
   * Default number of entities (i.e., records) to process in each batch of the reverse transformation before writing them to file.
   */
  public int batch_size = 1000;


  /**
   * Constructor of a ReverseConfiguration object.
   * @param path  Path to a properties file containing all parameters to be used in the transformation.
   */
  public ReverseConfiguration(String path) 
  {
	myAssistant = new Assistant();
    this.path = path;
    buildConfiguration();  
  }

  /**
   * Loads the reverse configuration from a properties file.
   */
  private void buildConfiguration() {
	  
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(path));
    } catch (IOException io) {
      System.out.println(Level.WARNING + " Problems loading reverse configuration file: " + io);
    }
    initializeParameters(properties);
  }

  /**
   * Initializes all the parameters for the reverse transformation.
   *
   * @param   All properties as specified in the configuration file.
   */
  private void initializeParameters(Properties properties) {
	  
	 //Format for output data: SHAPEFILE, CSV
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("outputFormat"))) {
		 outputFormat = properties.getProperty("outputFormat").trim();
	 }
	 
	 //File specification properties
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("inputFiles"))) {
		 inputFiles = properties.getProperty("inputFiles").trim();
	 }
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("outputFile"))) {
		 outputFile = properties.getProperty("outputFile").trim();
	 }
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("sparqlFile"))) {
		 sparqlFile = properties.getProperty("sparqlFile").trim();
	 }
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("tmpDir"))) {
		 tmpDir = properties.getProperty("tmpDir").trim();
	 }
	  
	 //RDF serialization property
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("serialization"))) {
		 serialization = properties.getProperty("serialization").trim();
	 }
		 
    //CSV specification properties
	if (!myAssistant.isNullOrEmpty(properties.getProperty("delimiter"))) {
        delimiter = properties.getProperty("delimiter").charAt(0);
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("quote"))) {
        quote = properties.getProperty("quote").trim().charAt(0);
      }	
	
	//Number of entities (i.e., records) to hold in each batch                           
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("batchSize"))) {
		 try {
			 batch_size = Integer.parseInt(properties.getProperty("batchSize").trim());
			 //Apply the default value in case of invalid settings
			 if ((batch_size < 1) || (batch_size > 10000))
				 batch_size = 1000;       
		 }
		 catch(Exception e) {
			 ExceptionHandler.abort(e, "Incorrect value set for batch size. Please specify a positive integer value in your configuration file.");
		 }		 
	 }
	 
	//Encoding of data values
	encoding = StandardCharsets.UTF_8.name();                   //Default encoding
	if (!myAssistant.isNullOrEmpty(properties.getProperty("encoding"))) {
		if (Charset.isSupported(properties.getProperty("encoding").trim()))
			encoding = Charset.forName(properties.getProperty("encoding").trim()).name();
		else
			System.err.println("Specified encoding " + properties.getProperty("encoding").trim() + " is not recognized.");
      }	
	else
		System.out.print("No encoding specified. Utilizing default ");
	
	System.out.println("Encoding: " + encoding);
		
	//Spatial reference system transformation properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("sourceCRS"))) {
    	sourceCRS = properties.getProperty("sourceCRS").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("targetCRS"))) {
    	targetCRS = properties.getProperty("targetCRS").trim();
      }
   
    //Default language specification tag for string literals
    if (!myAssistant.isNullOrEmpty(properties.getProperty("defaultLang"))) {
        defaultLang = properties.getProperty("defaultLang").trim();
      }
    
  }

}
