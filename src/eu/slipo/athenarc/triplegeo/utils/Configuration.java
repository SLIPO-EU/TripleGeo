/*
 * @(#) Configuration.java 	 version 1.3   28/11/2017
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;
//import java.util.logging.Logger;

/**
 * Class to parse configuration files used in the library.
 *
 * @author jonathangsc
 * initially implemented for geometry2rdf utility (source: https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF)
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */
public final class Configuration {

  Assistant myAssistant;
  public String mode;
  public String inputFormat;
  public String path;
  public String inputFiles;
  public String tmpDir;
  public String outputDir;
  public String mappingSpec;
  public String classificationSpec;
  public String serialization;
  public String targetOntology;
  public String prefixFeatureNS = "georesource";
  public String prefixGeometryNS = "geontology";
  public String nsGeometryURI = "http://www.opengis.net/ont/geosparql#";
  public String nsFeatureURI = "http://slipo.eu/resource/";
  public String pointType = "http://www.opengis.net/ont/sf#Point";
  public String lineStringType = "http://www.opengis.net/ont/sf#LineString";
  public String polygonType = "http://www.opengis.net/ont/sf#Polygon";
  public String defaultLang = "en";
  public String featureName;
  public String attrKey;
  public String valIgnore;
  public String tableName;
  public String filterSQLCondition;
  public String attrGeometry;
  public String attrName;
  public String attrCategory;
  public String attrX;
  public String attrY;
  public char delimiter;
  public char quote;
  public String encoding;
  public String sourceCRS;
  public String targetCRS;
  public String dbType;
  public String dbName;
  public String dbUserName;
  public String dbPassword;
  public String dbHost;
  public int dbPort;

  //private static final Logger LOG = Logger.getLogger(Configuration.class.getName());

  //Constructor
  public Configuration(String path) 
  {
	myAssistant = new Assistant();
    this.path = path;
    buildConfiguration();
    
  }

  /**
   * Loads the configuration from a properties file
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
   * Initializes all the parameters from the configuration.
   *
   * @param properties with the properties object.
   */
  private void initializeParameters(Properties properties) {
	 
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
	 
	 //Specification of mappings from input schema to RDF triples
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("mappingSpec"))) {
		 mappingSpec = properties.getProperty("mappingSpec").trim();
	 }

	 //Specification for classification hierarchy into categories for entities
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("classificationSpec"))) {
		 classificationSpec = properties.getProperty("classificationSpec").trim();
	 }
	 
	 //Output RDF serialization property
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("serialization"))) {
		 serialization = properties.getProperty("serialization").trim();
	 }
	 
	 //Spatial ontology of RDF geometries: GeoSPARQL, Virtuoso, or WGS84 Geoposition RDF vocabulary
	 if (!myAssistant.isNullOrEmpty(properties.getProperty("targetOntology"))) {
		 targetOntology = properties.getProperty("targetOntology").trim();
	 }
	 
	//Namespace specification properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("prefixFeatureNS"))) {
      prefixFeatureNS = properties.getProperty("prefixFeatureNS").trim();
    }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsFeatureURI"))) {
      nsFeatureURI = properties.getProperty("nsFeatureURI").trim();
    }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("prefixGeometryNS"))) {
      prefixGeometryNS = properties.getProperty("prefixGeometryNS").trim();
    }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("nsGeometryURI"))) {
      nsGeometryURI = properties.getProperty("nsGeometryURI").trim();
    }
    
    //Geometry type properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("pointType"))) {
      pointType = properties.getProperty("pointType").trim();
    }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("linestringType"))) {
      lineStringType = properties.getProperty("linestringType").trim();
    }
    if (!myAssistant.isNullOrEmpty(properties.getProperty("polygonType"))) {
      polygonType = properties.getProperty("polygonType").trim();
    }

    //Feature and attribute properties
    if (!myAssistant.isNullOrEmpty(properties.getProperty("featureName"))) {
    	featureName = properties.getProperty("featureName").trim();
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
    if (!myAssistant.isNullOrEmpty(properties.getProperty("valIgnore"))) {
    	valIgnore = properties.getProperty("valIgnore").trim();
      }
   
    //CSV specification properties
	if (!myAssistant.isNullOrEmpty(properties.getProperty("delimiter"))) {
        delimiter = properties.getProperty("delimiter").charAt(0);
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("quote"))) {
        quote = properties.getProperty("quote").trim().charAt(0);
      }	
	if (!myAssistant.isNullOrEmpty(properties.getProperty("attrX"))) {
        attrX = properties.getProperty("attrX").trim();
      }
	if (!myAssistant.isNullOrEmpty(properties.getProperty("attrY"))) {
        attrY = properties.getProperty("attrY").trim();
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
   
    //Language specification property
    if (!myAssistant.isNullOrEmpty(properties.getProperty("defaultLang"))) {
        defaultLang = properties.getProperty("defaultLang").trim();
      }
    
  }

}
