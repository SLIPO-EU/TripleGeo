/*
 * @(#) Configuration.java 	 version 1.7   28/2/2019
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;

/**
 * Parser of user-specified configuration files to be used during transformation of geospatial features into RDF triples.
 *
 * @author Kostas Patroumpas
 * @version 1.7
 */

/* DEVELOPMENT HISTORY
 * Initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Modified by: Kostas Patroumpas, 8/2/2013; adjusted to TripleGeo functionality
 * Modified by: Georgios Mandilaras, 28/12/2018; added parameterization for executions over Spark
 * Last modified: 28/2/2019
 */
public final class Configuration {

  Assistant myAssistant;
  
  /**
   * Execution platform at runtime: either JVM (default) or SPARK. JVM also supports multi-threaded (concurrent) execution.
   */
  public String runtime = "JVM";
  
  /**
   * Transformation mode: (disk-based) GRAPH, (in-memory) STREAM, (in-memory) XSLT, or RML.
   */
  public String mode;

  /**
   * Format of input data. Supported formats: SHAPEFILE, DBMS, CSV, GPX, GEOJSON, XML, OSM.
   */
  public String inputFormat;

  /**
   * Path to a properties file containing all parameters used in the transformation.
   */
  private String path;
  
  /**
   * Path to file(s) containing input dataset(s). 
   * Multiple input files (of exactly the same format and attributes) can be specified, separating them by ';' in order to activate multiple concurrent threads for their transformation.
   */
  public String inputFiles;
  
  /**
   * Path to a directory where intermediate files may be temporarily written during transformation.
   */
  public String tmpDir;
  
  /**
   * Path to the directory where output files will be written. By convention, output RDF files have the same name as their corresponding input dataset.
   */
  public String outputDir;
  
  /**
   * Default number of entities (i.e., records) to handle in each batch (applicable to STREAM and RML modes).
   */
  public int batch_size = 10;

  /**
   * Path to a file containing mappings of attributes from input schema to RDF properties.
   */
  public String mappingSpec;

  /**
   * Path to a file specifying a classification hierarchy with categories assigned to features in the dataset.
   */
  public String classificationSpec;

  /**
   * Specifies whether the join attribute with the classification scheme is based on the category identifier (false) or the actual name of the category (true). 
   * By default, transformation uses identifiers of categories in the classification scheme. This parameter has no effect if no classification hierarchy has been specified.
   */
  public boolean classifyByName = false;

  /**
   * Output RDF serialization. Supported formats: RDF/XML (default), RDF/XML-ABBREV, N-TRIPLES, TURTLE (or TTL), N3.
   */
  public String serialization;

  /**
   * Specifies the spatial ontology for geometries in the exported RDF data. 
   * Possible values: 1) "GeoSPARQL", 2) "Virtuoso" (legacy RDF ontology for points only), 3) "wgs84_pos" (for WGS84 Geoposition RDF vocabulary).
   */
  public String targetGeoOntology;
  
  /**
   * List of prefixes for namespaces employed in the transformation.
   */
  public String[] prefixes;

  /**
   * List of namespaces corresponding to the prefixes.
   */
  public String[] namespaces;
  
  /**
   * Ontology namespace of each transformed feature.
   */
  public String ontologyNS = "http://slipo.eu/def#";
  
  /**
   * Geometry namespace for WKT literals and spatial properties.
   */
  public String geometryNS = "http://www.opengis.net/ont/geosparql#";
  
  /**
   * Namespace for feature URIs.
   */
  public String featureNS = "http://slipo.eu/id/poi/";
  
  /**
   * Namespace for the URIs of the category assigned to each feature.
   */
  public String featureClassNS = "http://slipo.eu/id/term/";

  /**
   * Namespace for the classification scheme employed for the features.
   */
  public String featureClassificationNS = "http://slipo.eu/id/classification/";
  
  /**
   * Namespace for the data source URI assigned to each feature.
   */
  public String featureSourceNS = "http://slipo.eu/id/poisource/";

  /**
   * Default language specification tag for string literals.
   */
  public String defaultLang = "en";
  
  /**
   * String specifying the data source (provider) of the input dataset(s).
   */
  public String featureSource;

  /**
   * Name of the database table (or view) where the features will be retrieved from.
   */
  public String tableName;
  
  /**
   * SQL filter to be applied on the database table.
   */
  public String filterSQLCondition;
  
  /**
   * Name of the input attribute containing a unique identifier of each feature.
   */
  public String attrKey;
  
  /**
   * Name of the attribute containing the geometry of a feature.
   */
  public String attrGeometry;
  
  /**
   * Name of the attribute specifying the main name of a feature.
   */
  public String attrName;
  
  /**
   * Name of the attribute specifying the category of a feature according to a classification scheme.
   */
  public String attrCategory;
  
  /**
   * Name of the attribute containing the X-ordinate (or longitude) of a feature.
   * Ignored in case that attrGeometry has been specified.
   */
  public String attrX;

  /**
   * Name of the attribute containing the Y-ordinate (or latitude) of a feature.
   * Ignored in case that attrGeometry has been specified.
   */
  public String attrY;
  
  /**
   * Indicates whether to export a CSV file with records for the SLIPO Registry.
   * CAUTION! This functionality is applicable in the context of the SLIPO Workbench only.
   */
  public boolean registerFeatures = false;
  
  /**
   * Delimiter character for CSV file data sources.
   * Mandatory for CSV input; ignored otherwise.
   */
  public char delimiter;
  
  /**
   * Quote character for strings in CSV file data sources.
   * Mandatory for CSV input; ignored otherwise.
   */
  public char quote;
  
  /**
   * Encoding used for strings. 
   * Default encoding: UTF-8.
   */
  public String encoding;
  
  /**
   * Spatial reference system (EPSG code) of the input dataset. If omitted, geometries are assumed in WGS84 reference system (EPSG:4326).
   * Example: EPSG:2100
   */
  public String sourceCRS;
  
  /**
   * Spatial reference system (EPSG code) of geometries in the output RDF triples. If omitted, RDF geometries will retain their original georeference (i.e., no reprojection of coordinates will take place).
   * Example: EPSG:4326
   */
  public String targetCRS;
  
  /**
   * Type of database connection. Possible values: MSAccess; MySQL; Oracle; PostGIS; DB2; SQLServer; SpatiaLite.
   */
  public String dbType;

  /**
   * JDBC parameter for the name of the database to connect to.
   */
  public String dbName; 
  
  /**
   * JDBC parameter for the username in the database connection.
   */
  public String dbUserName;
  
  /**
   * JDBC parameter for the password in the database connection.
   */
  public String dbPassword;

  /**
   * JDBC parameter for the host name (or IP address) of the computer hosting the database.
   */
  public String dbHost;

  /**
   * JDBC parameter for the port number on which the database in the host computer accepts user requests.
   */
  public int dbPort;


  /**
   * Spark parameter for the number of partitions.
   */
  public int partitions;

  /**
    * Spark parameter specifying the logging level. Values INFO, WARN, and ERROR are allowed.
    */
  public String spark_logger_level;

 
  /**
   * Constructor of a Configuration object.
   * @param path   Path to a properties file containing all parameters to be used in the transformation.
   */
  public Configuration(String path) 
  {
	myAssistant = new Assistant();
    this.path = path;
    buildConfiguration();
    
  }

  /**
   * Loads the configuration from a properties file.
   */
  private void buildConfiguration() {
	  
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(path));
    } catch (IOException io) {
      System.out.println(Level.WARNING + " Problems loading configuration file: " + io);
    }
    initializeParameters(properties);
    
  }

  /**
   * Initializes all the parameters for the transformation from the configuration file.
   *
   * @param properties    All properties as specified in the configuration file.
   */
  private void initializeParameters(Properties properties) {

	 //Execution environment at runtime: either JVM or SPARK
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("runtime"))) {
		 runtime = properties.getProperty("runtime").trim();
	 }
		 
	 //Conversion mode: (disk-based) GRAPH, (in-memory) STREAM, (in-memory) XSLT, or RML
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("mode"))) {
		 mode = properties.getProperty("mode").trim();
	 }
	 
	 //Format of input data: SHAPEFILE, DBMS, CSV, GPX, GEOJSON, XML
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("inputFormat"))) {
		 inputFormat = properties.getProperty("inputFormat").trim();
	 }
	 
	 //File specification properties
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("inputFiles"))) {
		 inputFiles = properties.getProperty("inputFiles").trim();
	 }
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("outputDir"))) {
		 outputDir = properties.getProperty("outputDir").trim();
		 //Append a trailing slash to this directory in order to correctly create the path to output files
		 if ((outputDir.charAt(outputDir.length()-1)!=File.separatorChar) && (outputDir.charAt(outputDir.length()-1)!= '/'))
 			outputDir += "/";   //Always safe to use '/' instead of File.separator in any OS
	 }
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("tmpDir"))) {
		 tmpDir = properties.getProperty("tmpDir").trim();
	 }

	 //Number of entities (i.e., records) to handle in each batch; this actually controls how frequently the resulting RDF triples are written to file
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("batchSize"))) {
		 try {
		 batch_size = Integer.parseInt(properties.getProperty("batchSize").trim());
		 //Apply the default value in case of invalid settings
		 if ((batch_size < 1) || (batch_size > 1000))
			 batch_size = 10;       
		 }
		 catch(Exception e) {
			 ExceptionHandler.abort(e, "Incorrect value set for batch size. Please specify a positive integer value in your configuration file.");
		 }		 
	 }
		
	 //Path to a file containing attribute mappings from input schema to RDF properties
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("mappingSpec"))) {
		 mappingSpec = properties.getProperty("mappingSpec").trim();
	 }

	 //Path to a file specifying a classification hierarchy with categories assigned to features in the dataset
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("classificationSpec"))) {
		 classificationSpec = properties.getProperty("classificationSpec").trim();
	 }
	 
	 //Specifies whether the join attribute with the classification scheme is based on the category identifier (false) or the actual name of the category (true)
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("classifyByName"))) {
		 classifyByName = Boolean.parseBoolean(properties.getProperty("classifyByName"));
	 }
	 
	 //Output RDF serialization property
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("serialization"))) {
		 serialization = properties.getProperty("serialization").trim();
	 }
	 
	 //Spatial ontology of RDF geometries: GeoSPARQL, Virtuoso, or WGS84 Geoposition RDF vocabulary
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("targetGeoOntology"))) {
		 targetGeoOntology = properties.getProperty("targetGeoOntology").trim();
	 }
	 
	//NAMESPACE specification properties
	//Ontology namespace 
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsOntology"))) {
    	ontologyNS = properties.getProperty("nsOntology").trim();
    }
    //Geometry namespace
    if (!myAssistant.isNullOrEmpty(properties.getProperty("geometryNS"))) {
        geometryNS = properties.getProperty("geometryNS").trim();
    }
    //Namespace for feature URIs
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsFeatureURI"))) {
    	featureNS = properties.getProperty("nsFeatureURI").trim();
    }
    //Namespace for the category URIs assigned to each feature
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsClassURI"))) {
    	featureClassNS = properties.getProperty("nsClassURI").trim();
    }
    //Namespace for the classification scheme
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsClassificationURI"))) {
    	featureClassificationNS = properties.getProperty("nsClassificationURI").trim();
    }
    //Namespace for the data source URIs assigned to each feature
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsDataSourceURI"))) {
    	featureSourceNS = properties.getProperty("nsDataSourceURI").trim();
    }

	 //Reconstruct correspondence of prefixes and namespaces
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("prefixes"))) {
		 prefixes = properties.getProperty("prefixes").split(",");
	 }	 
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("namespaces"))) {
		 namespaces = properties.getProperty("namespaces").split(",");
	 }

    //Feature and attribute properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("featureSource"))) {
    	featureSource = properties.getProperty("featureSource").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("tableName"))) {
    	tableName = properties.getProperty("tableName").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("filterSQLCondition"))) {
    	filterSQLCondition = properties.getProperty("filterSQLCondition").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("attrKey"))) {
    	attrKey = properties.getProperty("attrKey").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("attrGeometry"))) {
    	attrGeometry = properties.getProperty("attrGeometry").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("attrCategory"))) {
    	attrCategory = properties.getProperty("attrCategory").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("attrName"))) {
    	attrName = properties.getProperty("attrName").trim();
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("attrX"))) {
        attrX = properties.getProperty("attrX").trim();
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("attrY"))) {
        attrY = properties.getProperty("attrY").trim();
      }
	
    //CUSTOM: Indicates whether to export a CSV file with records for the SLIPO Registry
    if ((properties.containsKey("registerFeatures")) && (!myAssistant.isNullOrEmpty(properties.getProperty("registerFeatures").trim()))) {
    	registerFeatures = Boolean.parseBoolean(properties.getProperty("registerFeatures").trim());
	 }
    
    //CSV specification properties
	if (!myAssistant.isNullOrEmpty(properties.getProperty("delimiter"))) {
        delimiter = properties.getProperty("delimiter").charAt(0);
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("quote"))) {
        quote = properties.getProperty("quote").trim().charAt(0);
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
    
    //Database connection properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbType"))) {
    	dbType = properties.getProperty("dbType").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbName"))) {
    	dbName = properties.getProperty("dbName").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbUserName"))) {
    	dbUserName = properties.getProperty("dbUserName").trim();
      }    
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbPassword"))) {
    	dbPassword = properties.getProperty("dbPassword").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbHost"))) {
    	dbHost = properties.getProperty("dbHost").trim();
      }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("dbPort"))) {
    	dbPort = Integer.parseInt(properties.getProperty("dbPort"));
      }
   
    //Default language specification tag for string literals
    if (!myAssistant.isNullOrEmpty(properties.getProperty("defaultLang"))) {
        defaultLang = properties.getProperty("defaultLang").trim();
      }

    //Number of partitions to be used for splitting input data; used either in multi-threaded execution or execution over Spark
    if (!myAssistant.isNullOrEmpty(properties.getProperty("partitions"))) {
        partitions = Integer.parseInt(properties.getProperty("partitions"));
    }
    else 
    	partitions = 1;         //Default: A single input file (i.e., no partitioning)

    //Level of Spark logging for performance; only used when executing transformations over Spark
    if (!myAssistant.isNullOrEmpty(properties.getProperty("spark_logger_level"))) {  
    	spark_logger_level = properties.getProperty("spark_logger_level").trim();
    }
    else     
    	spark_logger_level = "WARN";
  }

}