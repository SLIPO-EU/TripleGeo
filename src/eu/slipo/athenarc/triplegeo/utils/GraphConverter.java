/*
 * @(#) ModelConverter.java 	 version 1.2   2/8/2017
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
package eu.slipo.athenarc.triplegeo.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;


/**
 * Creates and populates a RDF model on disk so that data can be serialized into a file.
 * Created by: Kostas Patroumpas, 16/2/2013
 * Modified by: Kostas Patroumpas, 2/8/2017
 */
public class GraphConverter implements Converter {

	private static Configuration currentConfig; 
	
	public Model model;

		  /*
		   * Constructs aStreamVonverter Object.
		   *
		   * @param string - String with the path where the model will be written on disk.

		   */
		  public GraphConverter(Configuration config) {
			  
		      super();
		    
		      currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DB, etc.)  

		      //Remove any previously created files in the temporary directory
		      String dir = Assistant.removeDirectory(currentConfig.tmpDir);
		      File f = new File(dir);	      
		      f.mkdir();

		      Dataset dataset = TDBFactory.createDataset(dir) ;
		      
		      //The graph model to be used in the disk-based transformation process
		      this.model = dataset.getDefaultModel() ;
		      
		      //Preset some of the most common prefixes
		      this.model.setNsPrefix(currentConfig.prefixFeatureNS, currentConfig.nsFeatureURI);
		      this.model.setNsPrefix(currentConfig.prefixGeometryNS, currentConfig.nsGeometryURI);
		      this.model.setNsPrefix("geo", Constants.NS_GEO);
		      this.model.setNsPrefix("xsd", Constants.NS_XSD);
		      this.model.setNsPrefix("sf", Constants.NS_SF);
		      this.model.setNsPrefix("rdfs", Constants.NS_RDFS);
		      
		  }


		  //Return a handle to the created RDF mode
		  public Model getModel() {
			  
			  return this.model;
		  }

		  
		  //Used only for streaming results
		  public List<Triple> getTriples() {
			  
			  return null;
			  
		  }

		    /**
		     * 
		     * Handling non-spatial attributes (CURRENTLY supporting 'name' and 'type' attributes only)
		     */
		public void handleNonGeometricAttributes(String resType, String featureKey, String featureName, String featureClass) throws UnsupportedEncodingException, FileNotFoundException 
		    {		    	
		  	    try 
		  	    {
		  	        if (!featureKey.equals(currentConfig.valIgnore)) 
		  	        {
		  	          String encodingType =
		  	                  URLEncoder.encode(resType,
		  	                                    Constants.UTF_8).replace(Constants.STRING_TO_REPLACE,
		  	                                    		Constants.REPLACEMENT);
		  	          String encodingResource =
		  	                  URLEncoder.encode(featureKey,
		  	                                    Constants.UTF_8).replace(Constants.STRING_TO_REPLACE,
		  	                                    		Constants.REPLACEMENT);
		  	          String aux = encodingType + Constants.SEPARATOR + encodingResource;
		  	          
		  	          //Insert literal for name (name of feature)
		  	          if ((featureName != null) && (!featureName.equals(currentConfig.valIgnore)) && (!featureName.equals("")))  //NOT NULL values only
		  	          {
		  	        	insertNameTriple(
		  	        			  currentConfig.nsFeatureURI + aux,
		  	        			  currentConfig.nsFeatureURI + Constants.NAME,
		  	        			  featureName,
		  	        			  currentConfig.defaultLang);    //FIXME: Language literals should be according to specifications per record value, not for the attribute
		  	          }
		  	        
		  	          //Insert literal for class (type of feature)
		  	          if ((featureClass != null) && (!featureClass.equals(currentConfig.valIgnore)) && (!featureClass.equals("")))  //NOT NULL values only
		  	          {
		  	        	  encodingResource =
		  	                      URLEncoder.encode(featureClass,
		  	                                        Constants.UTF_8).replace(Constants.STRING_TO_REPLACE,
		  	                                        		Constants.REPLACEMENT);
		  	        	  

		  	              //Type according to application schema
		  	        	insertTypeTriple(
		  	            		currentConfig.nsFeatureURI + aux,
		  	            		currentConfig.nsFeatureURI + encodingResource);
		  	          
		  	          }
		  	        }
		  	    }
		  	    catch(Exception e) { e.printStackTrace(); }

		  }

		    
		    /**
		     * 
		     * Convert representation of a geometry into suitable triples
		     */
		    
		public void parseGeom2RDF(String t, String resource, String geom, int srid) 
		    {
		    	
		      try {
		      		
		        String encType = URLEncoder.encode(t, "utf-8").replace("+", Constants.REPLACEMENT);
		        String encResource = URLEncoder.encode(resource, "utf-8").replace("+", Constants.REPLACEMENT);
		        String aux = encType + "_" + encResource;

		        insertTypeTriple(currentConfig.nsFeatureURI + aux, currentConfig.nsFeatureURI
		                                     + URLEncoder.encode(t, "utf-8").replace("+", Constants.REPLACEMENT));
		        insertLabelTriple(currentConfig.nsFeatureURI + aux, resource);

		        //Distinguish geometric representation according to the target store (e.g., Virtuoso, GeoSPARQL compliant etc.)
		        if (currentConfig.targetOntology.equalsIgnoreCase("wgs84_pos"))
		        	insertWGS84Point(aux, geom);
		        else if (currentConfig.targetOntology.equalsIgnoreCase("Virtuoso"))
		        	insertVirtuosoPoint(aux, geom);
		        else
		        	insertWKTGeometry(aux, geom, srid);            //Encoding geometry with a specific CRS is allowed in GeoSPARQL only
		        
		        //Type according to GeoSPARQL feature
		        insertTypeTriple(
		        		currentConfig.nsFeatureURI + aux,
		        		currentConfig.nsGeometryURI + Constants.FEATURE);
		        
		      } catch (Exception e) {
		    	  e.printStackTrace();
		      }

		    }


		    /**
		     * 
		     * Insert a typical WKT geometry into the Jena model (suitable for GeoSPARQL compliant stores)
		     */
		private void insertWKTGeometry(String resource, String wkt, int srid) 
		    {		    	
		  	  //Detect geometry type from the WKT representation (i.e., getting the text before parentheses)
		  	  String geomType = " ";
		  	  int a = wkt.indexOf("(");
		  	  if (a>0)
		  		  geomType = wkt.substring(0, a).trim();
		  	
		  	insertHasGeometryTriple(currentConfig.nsFeatureURI + resource, Constants.NS_GEO + "hasGeometry",
		  				currentConfig.nsFeatureURI + Constants.FEAT + resource); 
		  		        
		  	insertTypeTriple(currentConfig.nsFeatureURI + Constants.FEAT + resource,
		  		           Constants.NS_SF + geomType);
		   
		  	//Encode SRID information before the WKT literal
		  	wkt = "<http://www.opengis.net/def/crs/EPSG/0/" + srid + "> " + wkt;

		  	insertLiteralTriple(
		  	    		currentConfig.nsFeatureURI + Constants.FEAT + resource,
		              Constants.NS_GEO + Constants.WKT,
		              wkt,
		              Constants.NS_GEO + Constants.WKTLiteral);
		  	    
		    }


		    /**
		     * 
		     * Insert a Point geometry according to Virtuoso RDF specifics into the Jena model
		     */
		private void insertVirtuosoPoint(String resource, String pointWKT) 
		    {  
		    	
		    	insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		          Constants.NS_POS + Constants.GEOMETRY,
		          pointWKT,       //geo.toText(),
		          Constants.NS_VIRT + Constants.GEOMETRY);   
		   
		    }
		  
		    
		    /**
		     * 
		     * Insert a Point geometry according to WGS84 Geoposition RDF vocabulary into the Jena model
		     */
		private void insertWGS84Point(String resource, String pointWKT) 
		    {

		      //Clean point WKT so as to retain its numeric coordinates only
		  	String[] parts = pointWKT.replace("POINT (","").replace(")","").split(Constants.BLANK);
		  	
		  	//Point p = (Point) geo;
		  	 
		  	insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		          Constants.NS_POS + Constants.LONGITUDE,
		          parts[0],     //String.valueOf(p.getX()),     //X-ordinate as a property
		          Constants.NS_XSD + "decimal");
		      
		    insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		              Constants.NS_POS + Constants.LATITUDE,
		              parts[1],   //String.valueOf(p.getY()),    //Y-ordinate as a property
		              Constants.NS_XSD + "decimal");
		   
		    }
		 

		    
		    /**
		     * 
		     * Insert a triple for the 'name' attribute of a feature
		     */
		private void insertNameTriple(String s, String p, String o, String lang) 
		    {

		  	  	Resource resource = this.model.createResource(s);
		  	    Property property = this.model.createProperty(p);
		  	    if (lang != null) {
		  	      Literal literal = this.model.createLiteral(o, lang);
		  	      resource.addLiteral(property, literal);
		  	    } else {
		  	      resource.addProperty(property, o);
		  	    }

		  	  }
		    
		      
		    /**
		     * 
		     * Insert a triple for the 'type' (i.e., class or characterization attribute) of a feature
		     */
		private void insertTypeTriple(String s, String o) 
		    {
		    	
		      Resource resource1 = this.model.createResource(s);
		      Resource resource2 = this.model.createResource(o);
		      this.model.add(resource1, RDF.type, resource2);
		      
		    }
		    
		    /**
		     * 
		     * Insert a triple for a literal value
		     */
		private void insertLiteralTriple(String s, String p, String o, String x) 
		    {
		   	
		      Resource resourceGeometry = this.model.createResource(s);
		      Property property = this.model.createProperty(p);
		      if (x != null) {
		        Literal literal = this.model.createTypedLiteral(o, x);
		        resourceGeometry.addLiteral(property, literal);
		      } else {
		        resourceGeometry.addProperty(property, o);
		      }   

		    }
   
		    
		    /**
		     * 
		     * Handle link between a spatial feature and its geometry
		     */
		private void insertHasGeometryTriple(String s, String p, String o) {
		   	
		      Resource resourceGeometry = this.model.createResource(s);
		      Property property = this.model.createProperty(p);
		      Resource resourceGeometry2 = this.model.createResource(o);
		      resourceGeometry.addProperty(property, resourceGeometry2);
		      
		    }

		        
		    /**
		     * 
		     * Insert a resource for a label
		     */
		private void insertLabelTriple(String resource, String label) {
		   	
		      Resource resource1 = this.model.createResource(resource);
		      this.model.add(resource1, RDFS.label, this.model.createLiteral(label, currentConfig.defaultLang));
		     
		    }

}
