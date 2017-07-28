/*
 * @(#) Configuration.java 	 version 1.2   11/4/2017
 *
 * Copyright (C) 2013 Institute for the Management of Information Systems, Athena RC, Greece.
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;


/**
 * Assistance class with various helper methods for TripleGeo
 * Created by: Kostas Patroumpas, 9/3/2013
 * Modified by: Kostas Patroumpas, 11/4/2017
 */

public class Assistant {

		//Determine the file extension of the output, depending on the RDF serialization
		public static String getOutputExtension(String serialization) {
			
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
			public static boolean isNullOrEmpty(String text) {
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
		public static String convertInputStreamToString(InputStream is)
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
		public static InputStream convertStringToInputStream(String string) {
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
		public static String getGMTime()
			{
				SimpleDateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
				gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

				//Current Date Time in GMT
				return gmtDateFormat.format(new java.util.Date()) + " GMT";
			}

		/**
		 * Notify progress while parsing input records
		 */
		public static void notifyProgress(int numRec) {
			
			if (numRec%1000 ==0)
			    System.out.print(Assistant.getGMTime() + " Processed " +  numRec + " records..." + "\r");
		}
		
		/**
		 * Report statistics regarding the conversion process
		 */
		public static void reportStatistics(long dt, int numRec, int numTriples, String serialization, String outputFile) {
			 System.out.println(Assistant.getGMTime() + " Thread completed successfully in " + dt + " ms. " + numRec + " records transformed into " + numTriples + " triples and exported to " + serialization + " file: " + outputFile + ".");
		}
		
		/**
		 * Removes all files from a given folder
		 */
		public static String removeDirectory(String path) 
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
		public static void saxonTransform(String sourcePath, String xsltPath, String resultPath) {  
		        TransformerFactory tFactory = TransformerFactory.newInstance();  
		        try 
		        {  
		            Transformer transformer = tFactory.newTransformer(new StreamSource(new File(xsltPath)));  
		            transformer.transform(new StreamSource(new File(sourcePath)), new StreamResult(new File(resultPath)));  
		            System.out.println("XSLT transformation completed successfully.\nOutput writen into file: " + resultPath );
		        } 
		        catch (Exception e) {  e.printStackTrace();  }  
		    } 
		    
	
}
