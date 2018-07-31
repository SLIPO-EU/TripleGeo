/*
 * @(#) RdfToCsv.java 	 version 1.5   26/2/2018
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
package eu.slipo.athenarc.triplegeo.tools;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.ReverseConfiguration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.ReverseConverter;
import eu.slipo.athenarc.triplegeo.utils.BatchReverseConverter;


/**
 * Main entry point of the utility for reconverting triples from a RDF graph into a CSV file.
 * LIMITATIONS: There are certain limitations in representing geometries and complex attribute values in CSV format: 
 *              - In case that there are mixed geometry types in input geometries (e.g., some are points while others are polygons), these are retained in the CSV file. 
 *              - Apart from a delimiter, configuration files for CSV records may also specify whether there is a quote character in string values
 *              - Reverse transformation discards attribute values that contain the quote or delimiter characters; otherwise, the CSV file may be malformed.
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 6/12/2017
 * Modified: 8/12/2017, added support for reprojection in another georeference system
 * Modified: 12/12/2017, fixed issue with string encodings; verified that UTF characters read and written correctly
 * Last modified: 26/2/2018
 */
public class RdfToCsv implements ReverseConverter {

	  BatchReverseConverter myReverseConverter;
	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  private WKTReader reader = null;
	  int sourceSRID;                               //Source CRS according to EPSG 
	  int targetSRID;                               //Target CRS according to EPSG
	  private ReverseConfiguration currentConfig;   //User-specified configuration settings
	  private String outputFile;                    //Output CSV file
	  private String encoding;                      //Encoding of the data records
	  private BufferedWriter writer;
	  String delimiter = "|";                       //Default delimiter
	  String quotes = "";                           //By default, use no quotes for string values
	  String attrGeom ;                             //Name of the geometry attribute in the resulting file
	  Integer numRecs;
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
		
	  /**
	   * Constructor for the reverse transformation process from RDF to CSV file.
	   * @param config  Parameters for Reverse Transformation.
	   * @param outFile   Path to the output CSV file.
	   * @param sourceSRID  Spatial reference system (EPSG code) of geometries in the input RDF triples
	   * @param targetSRID  Spatial reference system (EPSG code) of the output CSV file
	   */
	  public RdfToCsv(ReverseConfiguration config, String outFile, int sourceSRID, int targetSRID) {
       
		  myAssistant = new Assistant();
		  currentConfig = config;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      encoding = config.encoding;        //User-specified encoding; is not specified, UTF-8 is assumed
	      
	      try {
	    	  //Instantiate a writer for accommodating all resulting records
	    	  writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), encoding));
	    	  numRecs = 0;
	    	  
	    	  //Determine the quote character
	    	  if (config.quote != 0)           //Some character is specified in the configuration settings
	    		  if (config.quote == ';')
	    			  System.err.println("Character " + config.quote + " cannot be used in strings, because it is being used in EWKT representation of geometries. No quotes will be applied in string values.");
	    		  else
	    			  quotes = "" + config.quote;
	    	  
	    	  //Determine the delimiter to be used between attribute values
	    	  if (config.delimiter != 0)        //Some character is specified in the configuration settings
	    	  {   //Characters like comma or semicolon may be present in  string values
	    		  if (((config.delimiter == ',') || (config.delimiter == ';')) && (config.quote == 0))
	    		  {
	    			  System.err.println("All values will be quoted with \", since intended delimiter " + config.delimiter + " is used in EWKT representation of geometries.");
	    			  quotes = "\"";
	    		  }
	    		  delimiter = "" + config.delimiter;
	    	  }
	  
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Cannot create output file.");
	      }
	    	  
	      //Check if a coordinate transform is required for geometries
	      if (currentConfig.targetCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
	  	        
	  	        //Needed for parsing original geometry in WTK representation
	  	        GeometryFactory geomFactory = new GeometryFactory(new PrecisionModel(), sourceSRID);
	  	        reader = new WKTReader(geomFactory);
	  	        
	  		} catch (Exception e) {
	  			ExceptionHandler.abort(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
	  		}
	      else  //No transformation specified; determine the CRS of geometries
	      {
	    	  if (sourceSRID == 0)
	    	  {
	    		  this.sourceSRID = this.targetSRID = 4326;          //All features assumed in WGS84 lon/lat coordinates
	    		  System.out.println(Constants.WGS84_PROJECTION);
	    	  }
	    	  else
	    		  this.targetSRID = sourceSRID;    //Retain original CRS
	      }
	      
	      // Other parameters
	      if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
	    	  currentConfig.defaultLang = "en";
	      }

	  }


	  /**
	   * Closes output file, once all results have been written.
	   */
	  public void close() {		  
		  try {
			  writer.flush();
			  writer.close();
		  } catch (Exception e) { ExceptionHandler.abort(e, "Error in finalizing output file."); }
			
	  }

	  
	  /**
	   * Determine schema with all attribute names to be used as the header of CSV file. In case of CSV files, there is no need to specify data types for attributes.
	   * @param attrList  List of attribute names. Attributes in the schema will be created in the same order as specified in the SELECT query that fetches results from the RDF graph.
	   * @param sol  A single result (record) from a SELECT query in SPARQL.
	   * @return  True if attribute schema has been created successfully; otherwise, False.
	   */
	  public boolean createSchema(List<String> attrList, QuerySolution sol) {

	      try {	
	    	    int i = 0;
			    for (String r : attrList)
		        {   //ASSUMPTION: GeoSPARQL WKT literals always include their data type specification
			    	if ((sol.get(r) != null) && (sol.get(r).toString().contains("wktLiteral")))
			    		attrGeom = r;            //Single out the name of the geometry attribute
		        	writer.write(quotes + r + quotes);   
		        	i++;
		        	if (i < attrList.size())     //Append delimiter character after each attribute name, except for the last one
		        		writer.write(delimiter);
		        }
			    writer.newLine();
			    
			    return true;                 //Header has been created successfully
			
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Failed to initialize the output file.");
	  	  }
	      return false;
	      
	  }
	

	  /**
	   * Cleans up geometry literals and generates extended WKT (EWKT) representation, which includes the SRID (EPSG code). 
	   * @param g  A geometry WKT string.
	   * @return  An extended WKT (EWKT) representation of the geometry, which includes its SRID.
	   */
	  	public String getGeometrySpec(String g)  {
	  		
			//First, extract the EPSG code for the SRID
			String epsg = g.substring(g.lastIndexOf("EPSG/0/")+7, g.indexOf('>')).trim();	
			if ((epsg != "") && (sourceSRID != Integer.parseInt(epsg)))
			{
				try {
					throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
				} catch (Exception e) {
					ExceptionHandler.abort(e, "ERROR: Input geometries are not georeferenced in the CRS specified in the configuration!");
				}
			}
			else if ((epsg != "") && (myAssistant.isNullOrEmpty(currentConfig.targetCRS)))        //Geometries specify SRID and no reprojection is required
  				;
  			else if (targetSRID != 4326)                                    //Reprojection is required, so this must be specified in the shapefile
	  			epsg = "" + targetSRID;   		
  			else if ((targetSRID == 4326) || (epsg == ""))                  //Use WGS1984 as SRID if no spatial reference system is specified for geometries
	  			epsg = "4326";   		
  			
			//Input geometry is in EWKT, so SRID specification must be stripped off and converted to WKT
			String wkt = null;
			try {
				wkt = g.substring(g.indexOf('>')+1).trim();
				if (reproject != null)         //Apply on-the-fly reprojection, if specified
				{
					Geometry geom = myAssistant.WKT2GeometryTransform(wkt, reproject, reader);
					wkt = geom.toText();
				}
			} catch (Exception e) {
				ExceptionHandler.abort(e, "ERROR: Geometry cannot be reconstructed from WKT.");
			}

			return "SRID=" + epsg + ";" + wkt;    //EWKT representation; Geometry literal starts after the EPSG specification
//			return wkt;                           //ALTERNATIVE: return the WKT representation (without SRID)
		}
		

  	/**
  	 * Creates a feature collection from current batch of records and writes them into the output CSV file.
	 * @param attrList  List of attribute names.
	 * @param results  Collection of records in the current batch.
	 * @return  The number of records resulted in the current batch.
  	 */
	public Integer store(List<String> attrList, List<List<String>> results) {

	      try {
			    //Iterate through all results
			    for (List<String> rec: results)
			    {	
			    	for (int i=0; i < attrList.size(); i++)
			    	{
			    		if (attrList.get(i).equals(attrGeom))     //Properly identify geometry attribute for special handling of WKT literals
			    			writer.write(quotes + getGeometrySpec(rec.get(i)) + quotes);	
			    		else                                      //Handle all other attribute values
			    		{   //Check whether this attribute value contains delimiter or quote characters
			    			if (((!quotes.equals("")) && (rec.get(i).contains(quotes))) || ((quotes.equals("")) && (rec.get(i).contains(delimiter))))
			    			{
			    				System.err.println("Attribute value " + rec.get(i) + " discarded because it contains control characters used in the CSV file.");
			    				writer.write(quotes + quotes);
			    			}
			    			else
			    				writer.write(quotes + rec.get(i) + quotes);
			    		}
			    		if (i < attrList.size()-1)   //Append delimiter character after each attribute value, except for the last one
			    			writer.write(delimiter);
			    	}
			    	writer.newLine();
			    	numRecs++;
			    }				
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Failed to store records into the output file.");
	  	  }
	      
	      return numRecs;
	}

	@Override
	public String getGeometrySpec(Literal g) {
		return null;
	}
	  
}
