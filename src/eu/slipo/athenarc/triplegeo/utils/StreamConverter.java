/*
 * @(#) StreamConverter.java 	 version 1.2   2/8/2017
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

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;


/**
 * Provides a set of a streaming RDF triples in memory that can be readily serialized into a file.
 * Created by: Kostas Patroumpas, 9/3/2013
 * Modified by: Kostas Patroumpas, 2/8/2017
 */

public class StreamConverter implements Converter {

	private static Configuration currentConfig;
	
	private List<Triple> results = new ArrayList<>();       //An array of triples

		  /*
		   * Constructs a Stream Converter Object.
		   */
		  public StreamConverter(Configuration config) {
		    super();
		    currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DB, etc.) 
		  }

		  //Return triples resulting from conversion (usually from a single input feature)
		  public List<Triple> getTriples() {
			  
			  return results;
			  
		  }
		  
		  //Used only for a RDF model created on disk
		  public Model getModel() {
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
		  	        	results.add(insertNameTriple(
		  	        			  currentConfig.nsFeatureURI + aux,
		  	        			  currentConfig.nsFeatureURI + Constants.NAME,
		  	        			  featureName,
		  	        			  currentConfig.defaultLang));    //FIXME: Language literals should be according to specifications per record value, not for the attribute
		  	          }
		  	          
		  	          //Insert literal for class (type of feature)
		  	          if ((featureClass != null) && (!featureClass.equals(currentConfig.valIgnore)) && (!featureClass.equals("")))  //NOT NULL values only
		  	          {
		  	        	  encodingResource =
		  	                      URLEncoder.encode(featureClass,
		  	                                        Constants.UTF_8).replace(Constants.STRING_TO_REPLACE,
		  	                                        		Constants.REPLACEMENT);
		  	        	  

		  	              //Type according to application schema
		  	        	results.add(insertTypeTriple(
		  	            		currentConfig.nsFeatureURI + aux,
		  	            		currentConfig.nsFeatureURI + encodingResource));
		  	          
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

		        results.add(insertTypeTriple(currentConfig.nsFeatureURI + aux, currentConfig.nsFeatureURI
		                                     + URLEncoder.encode(t, "utf-8").replace("+", Constants.REPLACEMENT)));
		        results.add(insertLabelTriple(currentConfig.nsFeatureURI + aux, resource));

		        //Distinguish geometric representation according to the target store (e.g., Virtuoso, GeoSPARQL compliant etc.)
		        if (currentConfig.targetOntology.equalsIgnoreCase("wgs84_pos"))
		        	results.addAll(insertWGS84Point(aux, geom));
		        else if (currentConfig.targetOntology.equalsIgnoreCase("Virtuoso"))
		        	results.addAll(insertVirtuosoPoint(aux, geom));
		        else
		        	results.addAll(insertWKTGeometry(aux, geom, srid));            //Encoding geometry with a specific CRS is allowed in GeoSPARQL only
		        
		        //Type according to GeoSPARQL feature
		        results.add(insertTypeTriple(
		        		currentConfig.nsFeatureURI + aux,
		        		currentConfig.nsGeometryURI + Constants.FEATURE));
		        
		      } catch (Exception e) {
		    	  e.printStackTrace();
		      }

		    }


		    /**
		     * 
		     * Insert a typical WKT geometry into the Jena model (suitable for GeoSPARQL compliant stores)
		     */
		private List<Triple> insertWKTGeometry(String resource, String wkt, int srid) 
		    {
		    	List<Triple> results = new ArrayList<>();
		    	
		  	  //Detect geometry type from the WKT representation (i.e., getting the text before parentheses)
		  	  String geomType = " ";
		  	  int a = wkt.indexOf("(");
		  	  if (a>0)
		  		  geomType = wkt.substring(0, a).trim();
		  	
		  	results.add(insertHasGeometryTriple(currentConfig.nsFeatureURI + resource, Constants.NS_GEO + "hasGeometry",
		  				currentConfig.nsFeatureURI + Constants.FEAT + resource)); 
		  		        
		  	results.add(insertTypeTriple(currentConfig.nsFeatureURI + Constants.FEAT + resource,
		  		           Constants.NS_SF + geomType));
		   
		  	//Encode SRID information before the WKT literal
		  	wkt = "<http://www.opengis.net/def/crs/EPSG/0/" + srid + "> " + wkt;

		  	results.add(insertLiteralTriple(
		  	    		currentConfig.nsFeatureURI + Constants.FEAT + resource,
		              Constants.NS_GEO + Constants.WKT,
		              wkt,
		              Constants.NS_GEO + Constants.WKTLiteral));
		  	    
		  	  return results;         //An array of triples
		    }


		    /**
		     * 
		     * Insert a Point geometry according to Virtuoso RDF specifics into the Jena model
		     */
		private List<Triple> insertVirtuosoPoint(String resource, String pointWKT) 
		    {  
		    	List<Triple> results = new ArrayList<>();
		    	
		    	results.add(insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		          Constants.NS_POS + Constants.GEOMETRY,
		          pointWKT,       //geo.toText(),
		          Constants.NS_VIRT + Constants.GEOMETRY));
		    	
		    	return results;         //An array of triples (a single one in this case)
		   
		    }
		  
		    
		    /**
		     * 
		     * Insert a Point geometry according to WGS84 Geoposition RDF vocabulary into the Jena model
		     */
		private List<Triple> insertWGS84Point(String resource, String pointWKT) 
		    {
		    	List<Triple> results = new ArrayList<>();
		    	
		      //Clean point WKT so as to retain its numeric coordinates only
		  	String[] parts = pointWKT.replace("POINT (","").replace(")","").split(Constants.BLANK);
		  	
		  	//Point p = (Point) geo;
		  	 
		  	results.add(insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		          Constants.NS_POS + Constants.LONGITUDE,
		          parts[0],     //String.valueOf(p.getX()),     //X-ordinate as a property
		          Constants.NS_XSD + "decimal"));
		      
		      results.add(insertLiteralTriple(
		    		  currentConfig.nsFeatureURI + resource,
		              Constants.NS_POS + Constants.LATITUDE,
		              parts[1],   //String.valueOf(p.getY()),    //Y-ordinate as a property
		              Constants.NS_XSD + "decimal"));
		   
		      return results;         //An array of triples
		    }
		 

		    
		    /**
		     * 
		     * Insert a triple for the 'name' attribute of a feature
		     */
		private Triple insertNameTriple(String s, String p, String o, String lang) 
		    {
		        Triple t = new Triple(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createLiteral(o, lang));
		        return t;
		  	  }
		    
		      
		    /**
		     * 
		     * Insert a triple for the 'type' (i.e., class or characterization attribute) of a feature
		     */
		private Triple insertTypeTriple(String s, String o) 
		    {		      
		      Triple t = new Triple(NodeFactory.createURI(s), RDF.type.asNode(), NodeFactory.createURI(o));
		      return t;
		    }
		    
		    /**
		     * 
		     * Insert a triple for a literal value
		     */
		private Triple insertLiteralTriple(String s, String p, String o, String x) 
		    {
			  RDFDatatype geomDataType = TypeMapper.getInstance().getSafeTypeByName(x);
		      Triple t = new Triple(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createLiteral(o, geomDataType));
		      return t;
		    }

		    
		    
		    /**
		     * 
		     * Handle link between a spatial feature and its geometry
		     */
		private Triple insertHasGeometryTriple(String s, String p, String o) {
		   			      
		      Triple t = new Triple(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createURI(o));
		      return t;
		    }

		        
		    /**
		     * 
		     * Insert a resource for a label
		     */
		private Triple insertLabelTriple(String resource, String label) {
		   	
		      Triple t = new Triple(NodeFactory.createURI(resource), RDFS.label.asNode(), NodeFactory.createLiteral(label, currentConfig.defaultLang));
		      return t;
		    }

		  
}
