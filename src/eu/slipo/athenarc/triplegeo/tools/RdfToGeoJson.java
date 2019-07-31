/*
 * @(#) RdfToGeoJson.java	version 1.9   12/7/2019
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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.geotools.data.DataUtilities;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.ReverseConfiguration;
import eu.slipo.athenarc.triplegeo.utils.ReverseConverter;

/**
 * Main entry point of the utility for reconverting triples from a RDF graph into a GeoJson file.
 * LIMITATIONS: There are certain limitations of the GeoJson format:
 * 				- According to the latest GeoJSON specification (RFC7946), the CRS must be WGS84(lat/long).
 * 				- In case that there are mixed geometry types in input geometries (e.g., some are points while others are polygons), these are retained in the GeoJson file. 
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 12/7/2019
 * Last modified: 12/7/2019
 */
public class RdfToGeoJson implements ReverseConverter {

	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  WKTReader reader;
	  int sourceSRID;                                  //Source CRS according to EPSG 
	  int targetSRID;                                  //Target CRS according to EPSG
	  private ReverseConfiguration currentConfig;      //User-specified configuration settings
	  private String outputFile;                       //Output file
	  private String encoding;                         //Encoding of the data records
	  private BufferedWriter writer;
	  FeatureJSON fJSON;
	  
	  SimpleFeatureBuilder featureBuilder;
	  String geomType;
	  String typeName;
	  String attrGeom;                                 //Name of the geometry attribute in the resulting file
	  int numRecs;
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
		

	  /**
	   * Constructor for the reverse transformation process from RDF to GeoJson.
	   * @param config  Parameters for Reverse Transformation.
	   * @param outFile   Path to the output GeoJson file.
	   * @param sourceSRID  Spatial reference system (EPSG code) of geometries in the input RDF triples
	   * @param targetSRID  Spatial reference system (EPSG code) of the output GeoJson file
	   */
	  public RdfToGeoJson(ReverseConfiguration config, String outFile, int sourceSRID, int targetSRID) {
     
		  myAssistant = new Assistant();
		  currentConfig = config;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      encoding = config.encoding;        //User-specified encoding; is not specified, UTF-8 is assumed
		  
	      //Create a GeoJson file for accommodating all resulting records
	      //Schema and spatial reference system are defined later
	      try {
	    	  //Allow interaction with JSON objects (in this case, writing features to JSON) 
	    	  fJSON = new FeatureJSON();    	  
	    	  fJSON.setEncodeFeatureCollectionBounds(true);
	    	  fJSON.setEncodeFeatureCollectionCRS(true);
	    	  
	    	  //Instantiate a writer for accommodating all resulting records
	    	  writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), encoding));
	    	  writer.write("{\"type\": \"FeatureCollection\",\r\n");
	          numRecs = 0;
	          
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Cannot create output file.");
	      }
	      
	      //Check if a coordinate transform is required for geometries
	      //CAUTION! Reprojection to any valid CRS is supported, although the GeoJson standard assumes WGS84 georeference for features.
	      if (currentConfig.targetCRS != null)
	  	    try {
	  	        boolean lenient = true; // allow for some error due to different datums
	  	        CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
	  	        CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);    
	  	        reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
	  	        
		        //If specified, write the CRS to the header of the output file
	  	        writer.write("\"crs\": ");
		        fJSON.writeCRS(targetCRS, writer);
		        writer.write(",\r\n");
	  	      
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
	        
	      //Now can start collecting features and writing them to the file
	      try {
			writer.write("\"features\": [\r\n\t");
	      } catch (IOException e) { ExceptionHandler.abort(e, "Error while writing to output file."); }
	      
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
			  writer.write("]\r\n}");    //Close the collection
			  writer.flush();
			  writer.close();
		  } catch (Exception e) { ExceptionHandler.abort(e, "Error in finalizing output file."); }	
	  }

	  
	  /**
	   * Determine schema with all attribute names and their data types.
	   * @param attrList  List of attribute names. Attributes in the schema will be created in the same order as specified in the SELECT query that fetches results from the RDF graph.
	   * @param sol  A single result (record) from a SELECT query in SPARQL.
	   * @return  True if attribute schema has been created successfully; otherwise, False.
	   */
	  public boolean createSchema(List<String> attrList, QuerySolution sol) {

		  String typeSpec = "";
	      try {
		        for (int i=0; i < attrList.size(); i++)
		        {
		        	Literal r;
		        	if (sol.get(attrList.get(i)) == null) 
		        		r = null;
		        	else if (!sol.get(attrList.get(i)).isLiteral())             //Non-literal values are converted to string literals
		        		r = sol.get(attrList.get(i)).getModel().createLiteral(sol.get(attrList.get(i)).toString());
		        	else
		        		r = sol.getLiteral(attrList.get(i));	
		        	if (r != null)
		        	{   //Designating typical data types for attributes
		        		String d = r.getDatatypeURI().toLowerCase();

		        		if (d.contains("wktliteral"))        //ASSUMPTION: GeoSPARQL WKT literals always include their data type specification
		        		{
		        			attrGeom = attrList.get(i);      //Single out the name of the geometry attribute
		        			typeSpec += attrList.get(i) + ":Geometry";   //Always use the generic geometry data type to accommodate any type of geometries in the same file
//		        			Geometry g = getGeometry(r.toString());
		        		}
		        		else if ((d.contains("integer")) || (d.contains("int")) || (d.contains("short")) || (d.contains("long")))        				
		        			typeSpec += attrList.get(i) + ":Integer";
		        		else if (d.contains("float"))
		        			typeSpec = typeSpec + attrList.get(i) + ":Float";
		        		else if (d.contains("double"))
		        			typeSpec += attrList.get(i) + ":Double";
		        		else if (d.contains("date") && (!d.contains("time")))  //Shapefiles support dates, but neither time nor timestamp values
		        			typeSpec += attrList.get(i) + ":Date";
		        		else     //Values with no specific type will be cast as strings
		        			typeSpec += attrList.get(i) + ":String";
		        	}
		        	else
		        		typeSpec += attrList.get(i) + ":String";   //Values with no specific type will be cast as strings
		        	
		        	typeSpec += ",";        	
		        }
		        
		        //Strip off the last comma in the specification
		        typeSpec = typeSpec.substring(0, typeSpec.length()-1);
		        
		        //Data type specification for the GeoJson efile
		        SimpleFeatureType featTypes = DataUtilities.createType("GeoJsonSchema", typeSpec);
			    featureBuilder = new SimpleFeatureBuilder(featTypes);
			     
			    return true;                 //Schema has been created successfully
			
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Failed to initialize the output file.");
	  	  }
	      
	      return false;      
	  }


	  /**
	   * Cleans up geometry literals and generates its internal representation. 
	   * @param g  A geometry WKT string.
	   * @return  The internal representation of the geometry.
	   */
	  	public Geometry getGeometry(String g)  {
	  		
	  		Geometry geom = null;
			//First, extract the EPSG code for the SRID
			String epsg = g.substring(g.lastIndexOf("EPSG/0/")+7, g.indexOf('>')).trim();	
			if ((epsg != "") && (sourceSRID != Integer.parseInt(epsg)))
			{
				try {
					System.err.println("ERROR: Input geometries are not georeferenced in the CRS specified in the configuration!");
					throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
				} catch (Exception e) {
					ExceptionHandler.abort(e, "ERROR: Input geometries are not georeferenced in the CRS specified in the configuration!");
				}
			}
			else if ((epsg != "") && (myAssistant.isNullOrEmpty(currentConfig.targetCRS)))        //Geometries specify SRID and no reprojection is required
  				;
  			else if (targetSRID != 4326)                                    //Reprojection is required, so this must be specified in the output file
	  			epsg = "" + targetSRID;   		
  			else if ((targetSRID == 4326) || (epsg == ""))                  //Use WGS1984 as SRID if no spatial reference system is specified for geometries
	  			epsg = "4326";   		
  			
			//Input geometry is in EWKT, so SRID specification must be stripped off and converted to WKT
			String wkt = null;
			try {
				wkt = g.substring(g.indexOf('>')+1).trim();
				
				if (reproject != null)         //Apply on-the-fly reprojection, if specified
					geom = myAssistant.WKT2GeometryTransform(wkt, reproject, reader);
				else
					geom = myAssistant.WKT2Geometry(wkt);
				
				if (geom != null)
					myAssistant.updateMBR(geom);
			} catch (Exception e) {
				ExceptionHandler.abort(e, "ERROR: Geometry cannot be reconstructed from WKT.");
			}

			return geom;
		}
  	  
	  
	/**
	 * Creates a feature collection from current batch of records and writes them into the output GeoJson file.
	 * @param attrList  List of attribute names.
	 * @param results  Collection of records in the current batch.
	 * @return  The number of records resulted in the current batch.  
	 */
	public Integer store(List<String> attrList, List<List<String>> results) {

		boolean valid;
	      try {
			    //Iterate through all results in this batch in order to create the collection
			    for (List<String> rec: results)
			    {
			    	valid = true;
			    	for (int i=0; i < attrList.size(); i++)
			    	{
			    		if (attrList.get(i).equals(attrGeom))     //Properly identify geometry attribute for special handling of WKT literals
			    		{   //Input geometry is in EWKT, so SRID specification must be stripped off and converted to the internal representation
			    			Geometry g = getGeometry(rec.get(i));
			    			if (!g.isValid())
			    			{
			    				System.err.println(g.toText() + " is not valid. This feature will not be included in the output file.");
			    				featureBuilder.reset();
			    				valid = false;    //Mark this geometry as invalid
			    				break;
			    			}
			    			else
			    			{		    			    
			    				featureBuilder.add(g);    //Valid geometry
			    				myAssistant.updateMBR(g);
			    			}
			    		}
			    		else 
			    			featureBuilder.add(rec.get(i));   //All other attributes except geometry
			    	}

			    	if (valid)                 //Only valid geometries should be included in the GeoJson file
			    	{
			    		SimpleFeature feature = featureBuilder.buildFeature(""+numRecs); //Serial number is used as identifier in the output features;		    		
			    		fJSON.writeFeature(feature, writer);
			    		writer.write(",\r\n\t");
			    		numRecs++;
			    	}
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

	@Override
	public String getGeometrySpec(String g) {
		return null;
	}
	
}
