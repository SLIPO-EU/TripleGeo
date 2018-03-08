/*
 * @(#) Configuration.java 	 version 1.4  8/3/2018
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
import java.sql.Blob;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FilenameUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;


/**
 * Assistance class with various helper methods used in transformation or reverse transformation.
 * @author Kostas Patroumpas
 * @version 1.4
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 9/3/2013
 * Modified: 15/12/2017, included support for extra geometric transformations 
 * Modified: 9/2/2018, included additional execution statistics
 * TODO: Calculation of area and length for geometries must be done in Cartesian coordinates in order to return values in SI units (meters, square meters)
 * Last modified by: Kostas Patroumpas, 8/3/2018
 */

public class Assistant {

	public PGDBDecoder pgdbDecoder = null;         //Decoder of geometries read from a personal ESRI geodatabase (.mdb)
	public WKTReader wktReader = null;             //Parses a geometry in Well-Known Text format to a Geometry representation.
	
	private static Envelope mbr = null;            //Minimum Bounding Rectangle (in WGS84) of all geometries handled during a given transformation process
	

	/**
	 * Determines the serialization mode in the output RDF triples. Applicable in the RML transformation mode.
	 * @param serialization  A string with the user-specified serialization.
	 * @return  The RDFFormat to be used in the serialization.
	 */
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


	/**
	 * Determines the serialization mode in the output RDF triples. Applicable in GRAPH/STREAM transformation modes.
	 * @param serialization  A string with the user-specified serialization.
	 * @return  The RIOT language to be used in the serialization.
	 */
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
	
		//
	/**
	 * Determines the file extension of the output file(s), depending on the RDF serialization.
	 * @param serialization  A string with the user-specified serialization.
	 * @return
	 */
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
	 * Examines whether a given string is null or empty (blank).		
	 * @param text  The input string
	 * @return  True if the parameter is null or empty; otherwise, False.
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
	 * @param is  the InputStream.
	 * @return  A string with the content of the InputStream.
	 * @throws IOException
	 */
	public String convertInputStreamToString(InputStream is) throws IOException {
		
	    if (is != null) {
	      StringBuilder sb = new StringBuilder();
	      String line;
	      try {
	        BufferedReader reader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));
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
	 * Returns an InputStream with the content of the given string.
	 * @param string  A string to be converted.
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
	 * Get the current time (in the GMT time zone) to be used in user notifications.	
	 * @return  A timestamp value as string.
	 */
	public String getGMTime() {
		
		SimpleDateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		//Current Date Time in GMT
		return gmtDateFormat.format(new java.util.Date()) + " GMT";		
	}

	/**
	 * Notifies the user about progress in parsing input records.	
	 * @param numRec  The number of records processed so far.
	 */
	public void notifyProgress(int numRec) {
			
		if (numRec % 1000 == 0)
		    System.out.print(this.getGMTime() + " " + Thread.currentThread().getName() + " Processed " +  numRec + " records..." + "\r");
	}

	
	/**
	 * Report statistics upon termination of the transformation process.
	 * @param dt  The clock time (in milliseconds) elapsed since the start of transformation process.
	 * @param numRec  The total number of records that have been processed.
	 * @param numTriples  The number of RDF triples resulted from transformation.
	 * @param serialization  A string with the user-specified serialization of output triples.
	 * @param attrStatistics  Statistics collected per attribute during transformation.
	 * @param mode  Transformation mode, as specified in the configuration.
	 * @param outputFile  Path to the output file containing the RDF triples.
	 */
	public void reportStatistics(long dt, int numRec, int numTriples, String serialization, Map<String, Integer> attrStatistics, String mode, String outputFile) {
		
		 System.out.println(this.getGMTime() + " Thread " + Thread.currentThread().getName() + " completed successfully in " + dt + " ms. " + numRec + " records transformed into " + numTriples + " triples and exported to " + serialization + " file: " + outputFile + ".");
		 
		 if (mbr != null)
			 System.out.println("MBR of transformed geometries: X_min=" + mbr.getMinX() + ", Y_min=" + mbr.getMinY() + ", X_max=" + mbr.getMaxX() + ", Y_max=" + mbr.getMaxY());
		 
		 //Metadata regarding execution of this process
		 Map<String, Object> execStatistics = new HashMap<String, Object>();
		 execStatistics.put("Execution time (ms)", dt);
		 execStatistics.put("Input record count", numRec);
		 execStatistics.put("Output triple count", numTriples);
		 execStatistics.put("Output serialization", serialization);
		 execStatistics.put("Output file", outputFile);
		 execStatistics.put("Transformation mode", mode);
	
		 //MBR of transformed geometries
		 Map<String, Object> mapMBR = new HashMap<String, Object>();
		 if (mbr != null) {
			 mapMBR.put("X_min", mbr.getMinX());
			 mapMBR.put("Y_min", mbr.getMinY());
			 mapMBR.put("X_max", mbr.getMaxX());
			 mapMBR.put("Y_max", mbr.getMaxY());
		 }
		
		 //Compile all metadata together
		 Map<String, Object> allStats = new HashMap<String, Object>();
		 allStats.put("Execution Metadata", execStatistics);
		 allStats.put("MBR of transformed geometries (WGS84)", mapMBR);
		 allStats.put("Attribute Statistics", attrStatistics);
		 
	    //Convert metadata to JSON and write to a file
	    try {
	    	ObjectMapper mapper = new ObjectMapper();
	        mapper.writeValue(new File(FilenameUtils.removeExtension(outputFile) + "_metadata.json"), allStats);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }	
	}


	
	/**
	 * Removes all files from a given directory. Used for removing intermediate files created during transformation.	
	 * @param path The path to the directory.
	 * @return  The path to a temporary directory that holds intermediate files.
	 * @throws IOException 
	 */
	public String removeDirectory(String path){

	      File f = new File(path);

	      if (f.isDirectory()) {
	        if (f.exists()) {
	          String[] myfiles = f.list();
	          if (myfiles.length > 0) {
	            for (int i = 0; i < myfiles.length; i++) {
	              File auxFile = new File(path + "/" + myfiles[i]);     //Always safe to use '/' instead of File.separatorChar in any OS
//				  System.out.println("Removing ... " + auxFile.getPath());
	              if (auxFile.isDirectory())      //Recursively delete files in subdirectory
	            	  removeDirectory(auxFile.getPath());
	              auxFile.delete();
	            }
	          }
	          f.delete();
	        }
	      }

	      return path;	
	}

	/**
	 * Creates a temporary directory under the specified path. Used for holding intermediate files created during transformation.	
	 * @param path The path to the directory.
	 * @return  The path to a temporary directory that holds intermediate files.
	 */
	public String createDirectory(String path) {
		
		String dir = path;

		if ((path.charAt(path.length()-1)!=File.separatorChar) && (path.charAt(path.length()-1)!= '/'))
	        dir = path + "/";         //Always safe to use '/' instead of File.separatorChar in any OS

	      File f = new File(dir);

	      if (!f.isDirectory()) {
	        f.mkdir();
	      }

	      dir = dir + UUID.randomUUID() + "/";    //Generate a subdirectory that will be used for thread-safe graph storage when multiple threads are employed	      
	      
	      File fl = new File(dir);	      
		  fl.mkdir();
		  
	      return dir;	
	}

	
	/**
	 * Simple transformation method using Saxon XSLT 2.0 processor. 
	 * @param sourcePath  Absolute path to source GML file. 
	 * @param xsltPath  Absolute path to XSLT file. 
	 * @param resultPath  Absolute path to the resulting RDF file. 
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
        	ExceptionHandler.abort(e, "An error occurred during XSLT transformation.");      //Execution terminated abnormally
        }    
	} 
	
 
	/**
	 * Updates the MBR of the geographic dataset as this is being transformed. 	
	 * @param g  Geometry that will be checked for possible expansion of the MBR of all features processed thus far
	 */
	private static void updateMBR(Geometry g) {
			
		Envelope env = g.getEnvelopeInternal();          //MBR of the given geometry
		if (mbr == null)
			mbr = g.getEnvelopeInternal();
		else if (!mbr.contains(env))
			mbr.expandToInclude(env);                    //Expand MBR is the given geometry does not fit in
	}
		

	/**
	 * Transforms a WKT representation of a geometry into another CRS (reprojection).	
	 * @param wkt  Input geometry in Well-Known Text.
	 * @param transform  Parameters for the transformation, including source and target CRS.
	 * @return  WKT of the transformed geometry.
	 */
	public String wktTransform(String wkt, MathTransform transform) {  
	  	      
	   Geometry o = null;
       //Apply transformation
        try {
        	o = wktReader.read(wkt);
        	o = JTS.transform(o, transform);

        	//Update the MBR of all geometries processed so far
  	        //updateMBR(o);
		} catch (Exception e) {
			e.printStackTrace();
		}
         
        return o.toText();     //Get transformed WKT representation	
	}
	
	/** 
	 * Transforms an original geometry into another CRS (reprojection).
	 * @param g  Input geometry
	 * @param transform   Parameters for the transformation, including source and target CRS
	 * @return the transformed geometry
	 */
	public Geometry geomTransform(Geometry g, MathTransform transform) {  
	  	     
       //Apply transformation
        try {
        	g = JTS.transform(g, transform);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        return g;     //Return transformed geometry
	}

	
	/**
	 * Instantiates a new PGDBDecoder for geometries from a Personal ESRI geodatabase (MS Access format).
	 */
	public boolean initPGDBDecoder() {
		
		try {
			pgdbDecoder = new PGDBDecoder();
			return true;
		}
		catch (Exception e) {
			ExceptionHandler.abort(e, "Failed to initialize decoder for geometries in a personal geodatabase.");
		}
		return false;
	}
		
	
	/**
	 * Transforms a BLOB representation of a geometry contained in an ESRI personal geodatabase (.mdb) into WKT.
	 * @param blob  Input geometry BLOB
	 * @param transform  Parameters for the (optional) transformation, including source and target CRS
	 * @return  the WKT of the transformed geometry
	 */
	public String blob2WKT(Blob blob, MathTransform transform) {  

		String wkt = null;
		try {
			byte[] bytes = blob.getBytes(1, (int) blob.length());
			
			//Turn BLOB into geometry; this is not typical WKB, but a custom binary representation 
			Geometry geometry = pgdbDecoder.decodeGeometry(bytes);
			
			//CRS transformation
	      	if (transform != null)
	      		geometry = geomTransform(geometry, transform);
	      	
	      	//Update the MBR of all geometries processed so far
  	        //updateMBR(geometry);
  	        
	      	wkt = geometry.toText();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return wkt;   //Return (transformed) geometry in WKT representation
	}
		
	/** 
	 * Provides the Well-Known Text representation of a geometry for its inclusion as literal in RDF triples.
	 * @param g  Input geometry
	 * $param geomOntology  RDF ontology used in geometry representations (GeoSPARQL, Virtuoso geometry RDF, or WGS84 Geoposition RDF vocabulary)
	 * @return  WKT of the geometry
	 */	
	public String geometry2WKT(Geometry g, String geomOntology) {  
	  	     
	    WKTWriter writer = new WKTWriter();
	    String wkt;

        //Convert geometry into WKT
        if (geomOntology.toLowerCase().equals("geosparql"))
        	wkt = writer.writeFormatted(g);        //GeoSPARQL WKT representation for various types of geometries
        else
        	wkt = g.toText();                      //WKT representation for Virtuoso or WGS84 GeoPosition RDF vocabulary
            
        //Update the MBR of all geometries processed so far
        //updateMBR(g);
      
        return wkt;     //Return transformed geometry in WKT representation
	}	

	/** 
	 * Returns the internal geometry representation according to its Well-Known Text serialization.
	 * @param wkt  WKT of the geometry
	 * @return  A geometry object
	 */
	public static Geometry WKT2Geometry(String wkt) {  
	  	    
		WKTReader wktReader = new WKTReader();
		Geometry g = null;
        try {
        	g = wktReader.read(wkt);

        	//Update the MBR of all geometries processed so far
  	        //updateMBR(g);
		} catch (Exception e) {
			e.printStackTrace();
		}
    
        return g;     //Return geometry
	}	
		
	/** 
	 * Returns the area of a polygon geometry from its the Well-Known Text representation.
	 * @param polygonWKT  WKT of the polygon
	 * @return  calculated area (in the same units as the CRS of the geometry)
	 */
	public static double getArea(String polygonWKT) {
			
		Geometry g = WKT2Geometry(polygonWKT);
		return g.getArea();		                //Calculate the area of the given (multi)polygon 	
	}

	/** 
	 * Returns the length of a linestring or the perimeter of a polygon geometry from its the Well-Known Text representation.
	 * @param wkt WKT of the linestring or polygon
	 * @return  calculated length/perimeter (in the same units as the CRS of the geometry)
	 */	
	public static double getLength(String wkt) {
		
		Geometry g = WKT2Geometry(wkt);
		return g.getLength();		                //Calculate the length of the given (multi)linestring or the perimeter of the given (multi)polygon
	}
	
	/** 
	 * Returns a pair of lon/lat coordinates (in WGS84) of a geometry as calculated from its the Well-Known Text representation.
	 * @param wkt   WKT of the geometry
	 * @param targetSRID   the EPSG code of the CRS of this geometry 
	 * @return  An array with the pair of lon/lat coordinates
	 */
	public static double[] getLonLatCoords(String wkt, int targetSRID) {

	    WKTReader wktReader = new WKTReader();
	    Geometry g = null;
        try {
        	g = wktReader.read(wkt);
        	if (targetSRID != 4326)                   //In case that geometry is NOT georeferenced in WGS84, ...
        	{                                         //... it should be transformed in order to calculate its lon/lat coordinates
        		CoordinateReferenceSystem origCRS = CRS.decode("EPSG:" + targetSRID); //The CRS system of the original geometry
        		CoordinateReferenceSystem finalCRS = CRS.decode("EPSG:4326");         //CRS for WGS84
        		//Define a MathTransform object and apply it
        		MathTransform transform = CRS.findMathTransform(origCRS, finalCRS);
        		g = JTS.transform(g, transform); 	        		
        	}
        	
        	//Update the MBR of all geometries processed so far
  	        updateMBR(g);
        	
        	//Calculate the coordinates of its centroid	        	
        	return new double[] {g.getCentroid().getX(), g.getCentroid().getY()};
        	
		} catch (Exception e) {
			System.out.println("Geometry " + wkt + " is problematic.");
			e.printStackTrace();
		}
        
	    return null;
	}
		
	/** 
	 * Provides the internal geometry representation based on its the Well-Known Text, also applying transformation into another CRS (if required).
	 * @param wkt   WKT of the geometry
	 * @param transform    Parameters for the transformation, including source and target CRS
     * @param wktReader  Parser of WKT geometry representations
	 * @return  The transformed geometry
	 */	
	public Geometry WKT2GeometryTransform(String wkt, MathTransform transform, WKTReader wktReader) {  
	  	    
			Geometry g = null;
  	        try {
  	        	g = wktReader.read(wkt);
  	        	g = JTS.transform(g, transform);
  			} catch (Exception e) {
  				e.printStackTrace();
  			}
        
  	        return g;     //Return geometry
	}
		
	/** 
	 * Merges several input files into a single output file.
	 * @param inputFiles  List of the paths to the input files
	 * $param outputFile Path to merged output file
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
			ExceptionHandler.abort(e, "Output files were not merged.");
		}
	}
		
	
	/**
	 * Provides a UUID (Universally Unique Identifier) that represents a 128-bit long value to be used in the URI of a transformed feature.
     * Also known as GUID (Globally Unique Identifier).
	 * @param id  A unique identifier of the feature.
	 * @return The auto-generated UUID.
	 */
	public UUID getUUID(String id) {
			
		UUID uuid = null;
		
		//OPTION #1: A secure random UUID with minimal chance of collisions with existing ones
		//uuid = UUID.randomUUID();
		
		//OPTION #2: UUIDs generated by hashing over the identifier
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
	 * @param s  input string value
	 * @param encoding  name of the character encoding
	 * @return  output encoded string
	 */
	public String encodeUTF(String s, String encoding) {
			
		byte[] ptext = s.getBytes(); //Charset.forName(encoding)); 
		String value = new String(ptext, Charset.forName("UTF-8"));
		 
		return value;
	}

}
