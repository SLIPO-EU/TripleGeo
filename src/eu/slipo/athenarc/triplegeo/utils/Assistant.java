/*
 * @(#) Configuration.java 	 version 1.3  24/11/2017
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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.geotools.geometry.jts.JTS;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;


/**
 * Assistance class with various helper methods for TripleGeo
 * Created by: Kostas Patroumpas, 9/3/2013
 * Modified by: Kostas Patroumpas, 24/11/2017
 */

public class Assistant {

	//Determine the serialization mode in the output, depending on the given user specification (as a string)
	public org.openrdf.rio.RDFFormat getRDFSerialization(String serialization) {
			    
	    switch (serialization.toUpperCase()) {

        case "TURTLE": 
             return org.openrdf.rio.RDFFormat.TURTLE;
        case "TTL": 
             return org.openrdf.rio.RDFFormat.TURTLE;
        case "N3": 
             return org.openrdf.rio.RDFFormat.N3;
        case "TRIG": 
             return org.openrdf.rio.RDFFormat.TRIG;
        case "N-TRIPLES": 
             return org.openrdf.rio.RDFFormat.NTRIPLES;
        case "RDF/XML-ABBREV": 
             return org.openrdf.rio.RDFFormat.RDFA;
        case "RDF/XML": 
             return org.openrdf.rio.RDFFormat.RDFXML;
        default: 
             return org.openrdf.rio.RDFFormat.NTRIPLES;
        }
	    			
	}

	//Determine the serialization mode in the output, depending on the given user specification (as a string)
	public org.apache.jena.riot.Lang getRDFLang(String serialization) {
			    
	    switch (serialization.toUpperCase()) {

        case "TURTLE": 
             return org.apache.jena.riot.Lang.TURTLE;
        case "TTL": 
             return org.apache.jena.riot.Lang.TURTLE;
        case "N3": 
             return org.apache.jena.riot.Lang.N3;
        case "TRIG": 
             return org.apache.jena.riot.Lang.TRIG;
        case "N-TRIPLES": 
             return org.apache.jena.riot.Lang.NTRIPLES;
        case "RDF/XML-ABBREV": 
             return org.apache.jena.riot.Lang.RDFXML;
        case "RDF/XML": 
             return org.apache.jena.riot.Lang.RDFXML;
        default: 
             return org.apache.jena.riot.Lang.NTRIPLES;
        }
	    			
	}
	
		//Determine the file extension of the output, depending on the RDF serialization
		public String getOutputExtension(String serialization) {
			
			switch(serialization.toUpperCase()) {	
				case "TURTLE":
					return ".ttl";
				case "TTL":
					return ".ttl";
				case "N3":
					return ".ttl";
				case "TRIG":
					return ".trig";
				case "N-TRIPLES":
					return ".nt";
				case "RDF/XML-ABBREV":
					return ".rdf";
				case "RDF/XML":
					return ".rdf";
				default:
					return ".rdf";				
			}				
		}
		  
		  
		  /**
		   * Returns true if the parameter is null or empty. false otherwise.
		   *
		   * @param text
		   * @return true if the parameter is null or empty.
		   */
			public boolean isNullOrEmpty(String text) {
		    if (text == null || text.equals("")) {
		      return true;
		    } else {
		      return false;
		    }
		  }

		  /**
		   * Returns a String with the content of the InputStream
		   * @param is with the InputStream
		   * @return string with the content of the InputStream
		   * @throws IOException
		   */
		public String convertInputStreamToString(InputStream is)
		          throws IOException {
		    if (is != null) {
		      StringBuilder sb = new StringBuilder();
		      String line;
		      try {
		        BufferedReader reader = new BufferedReader(
		                new InputStreamReader(is, Constants.UTF_8));
		        while ((line = reader.readLine()) != null) {
		          sb.append(line).append(Constants.LINE_SEPARATOR);
		        }
		      } finally {
		        is.close();
		      }
		      return sb.toString();
		    } else {
		      return "";
		    }
		  }

		  /**
		   * Returns am InputStream with the parameter.
		   *
		   * @param string
		   * @return InputStream with the string value.
		   */
		public InputStream convertStringToInputStream(String string) {
		    InputStream is = null;
		    try {
		      is = new ByteArrayInputStream(string.getBytes("UTF-8"));
		    } catch (UnsupportedEncodingException e) {
		      e.printStackTrace();
		    }
		    return is;
		  }


		  /**
		   * Get the current GMT time for user notification.
		   *
		   * @return timestamp value as string.
		   */
		public String getGMTime()
			{
				SimpleDateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

				//Current Date Time in GMT
				return gmtDateFormat.format(new java.util.Date()) + " GMT";
			}

		/**
		 * Notify progress while parsing input records
		 */
		public void notifyProgress(int numRec) {
			
			if (numRec%1000 ==0)
			    System.out.print(this.getGMTime() + " Processed " +  numRec + " records..." + "\r");
		}
		
		/**
		 * Report statistics regarding the conversion process
		 */
		public void reportStatistics(long dt, int numRec, int numTriples, String serialization, String outputFile) {
			 System.out.println(this.getGMTime() + " Thread completed successfully in " + dt + " ms. " + numRec + " records transformed into " + numTriples + " triples and exported to " + serialization + " file: " + outputFile + ".");
		}
		
		/**
		 * Removes all files from a given folder
		 */
		public String removeDirectory(String path) 
			{
				String dir = path;

			    if (!dir.endsWith("/")) {
			        dir = dir + "/";
			      }
	
			      File f = new File(dir);
	
			      if (!f.isDirectory()) {
			        f.mkdir();
			      }
	
			      dir = dir + "TDB/";
			      
			      f = new File(dir);
	
			      if (f.isDirectory()) {
			        if (f.exists()) {
			          String[] myfiles = f.list();
			          if (myfiles.length > 0) {
			            for (int i = 0; i < myfiles.length; i++) {
			              File auxFile = new File(dir + myfiles[i]);
			              auxFile.delete();
			            }
			          }
			          f.delete();
			        }
			      }	
			      
//			      System.out.println("Removed files from " + dir);
			      return dir;
			}
			
			/** 
		     * Simple transformation method using Saxon XSLT 2.0 processor. 
		     * @param sourcePath - Absolute path to source GML file. 
		     * @param xsltPath - Absolute path to XSLT file. 
		     * @param resultPath - Absolute path to the resulting RDF file. 
		     */  
		public void saxonTransform(String sourcePath, String xsltPath, String resultPath) {  
			//Set saxon as the transformer engine for XML/GML datasets  
	        System.setProperty("javax.xml.transform.TransformerFactory", "net.sf.saxon.TransformerFactoryImpl"); 
	        
			TransformerFactory tFactory = TransformerFactory.newInstance();  
		        try 
		        {  
		            Transformer transformer = tFactory.newTransformer(new StreamSource(new File(xsltPath)));  
		            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		            transformer.setParameter("encoding", "UTF-8");
		            transformer.transform(new StreamSource(new File(sourcePath)), new StreamResult(new File(resultPath)));  
		            System.out.println("XSLT transformation completed successfully.\nOutput writen into file: " + resultPath );
		        } 
		        catch (Exception e) {  
		        	ExceptionHandler.invoke(e, "An error occurred during XSLT transformation.");      //Execution terminated abnormally
		        }  
		    } 
	
		
		/** 
		 * Attempt to transform a WKT representation of a geometry into another CRS
		 * @param wkt - Input geometry in Well-Known Text
		 * @param transform  - Parameters for the transformation, including source and target CRS
		 * @param wktReader - Reader of geometry features
		 * @return - WKT of the transformed geometry
		 */
		public String wktTransform(String wkt, MathTransform transform, WKTReader wktReader) {  
	  	      
			   Geometry o = null;
	  	       //Apply transformation
	  	        try {
	  	        	o = wktReader.read(wkt);
	  	        	o = JTS.transform(o, transform);
	  			} catch (Exception e) {
	  				e.printStackTrace();
	  			}
	  	         
	  	        return o.toText();     //Get transformed WKT representation
		}
	
		/** 
		 * Attempt to transform an original geometry into another CRS
		 * @param g - Input geometry
		 * @param transform  - Parameters for the transformation, including source and target CRS
		 * @return - the transformed geometry
		 */
		public Geometry geomTransform(Geometry g, MathTransform transform) {  
	  	     
  	       //Apply transformation
  	        try {
  	        	g = JTS.transform(g, transform);
  			} catch (Exception e) {
  				e.printStackTrace();
  			}
  	        
  	        return g;     //Return transformed geometry in WKT representation
		}
		
		/** 
		 * Provide the Well-Known Text representation of a geometry
		 * @param g - Input geometry
		 * $param geomSerialization - RDF serialization for geometries (GeoSPARQL, Virtuoso geometry RDF, or WGS84 Geoposition RDF vocabulary)
		 * @return - WKT of the geometry
		 */
		public String geometry2WKT(Geometry g, String geomSerialization) {  
	  	     
		    WKTWriter writer = new WKTWriter();
		    String wkt;

  	        //Convert geometry into WKT
  	        if (geomSerialization.toLowerCase().equals("geosparql"))
  	        	wkt = writer.writeFormatted(g);        //GeoSPARQL WKT representation for various types of geometries
  	        else
  	        	wkt = g.toText();                      //WKT representation for Virtuoso or WGS84 GeoPosition RDF vocabulary
  	        
  	        return wkt;     //Return transformed geometry in WKT representation
		}	

		/** 
		 * Merge a list of input files into a single output file
		 * @param inputFiles - List of (paths to) input files
		 * $param outputFile - Path to output file
		 */		
		public void mergeFiles(List<String> inputFiles, String outputFile) {
			OutputStream out;
			InputStream in;
			try {
				out = new FileOutputStream(outputFile);
				byte[] buf = new byte[1000];
			    for (String file : inputFiles) {
			        in = new FileInputStream(file);
			        int b = 0;
			        while ( (b = in.read(buf)) >= 0) {
			            out.write(buf, 0, b);
			            out.flush();
			        }
			    }
			    out.close();
			} catch (Exception e) {
				ExceptionHandler.invoke(e, "Output files were not merged.");
			}    
		}
		
		/** 
		 * Provides a UUID (Universally Unique Identifier) that represents a 128-bit long value
         * Also known as GUID (Globally Unique Identifier) 
		 * @param id: a unique identifier of the feature (possibly including a namespace as a prefix)
		 */		
		public UUID getUUID(String id) {
			UUID uuid = null;
			
			//OPTION #1: A secure random UUID with minimal chance of collisions with existing ones
			//uuid = UUID.randomUUID();
			
			//OPTION #2: UUIDs generated by hashing over the namespace and identifier
			try {
			    byte[] bytes = id.getBytes("UTF-8");
				uuid = UUID.nameUUIDFromBytes(bytes);
			} catch (Exception e) {
				e.printStackTrace();
			}
			 
			return uuid;
		}
		
		/** 
		 * Provides a UTF-8 encoding of the given string originally represented in the given encoding
		 * @param s: string value
		 * @param encoding: name of the character encoding
		 */		
		public String encodeUTF(String s, String encoding) {
			
			byte[] ptext = s.getBytes(); //Charset.forName(encoding)); 
			String value = new String(ptext, Charset.forName("UTF-8"));
			 
			return value;
		}
}
