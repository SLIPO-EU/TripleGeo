/*
 * @(#) RdfToShp.java 	 version 1.5   8/3/2018
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


import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
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
import eu.slipo.athenarc.triplegeo.utils.ReverseConfiguration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;
import eu.slipo.athenarc.triplegeo.utils.ReverseConverter;
import eu.slipo.athenarc.triplegeo.utils.BatchReverseConverter;;

/**
 * Main entry point of the utility for reconverting triples from a RDF graph into an ESRI Shapefile.
 * LIMITATIONS: The Shapefile format has several limitations:
 *              - The GEOMETRY attribute is always declared first, and can only store geometries of a single type;
 *              - Geometries must be of type Point, MultiPoint, MuiltiLineString, MultiPolygon;
 *              - Attribute names are limited in length (max 10 characters); otherwise, they get truncated;
 *              - Not all data types can be supported (e.g., Timestamp values must be represented as Strings).
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 7/12/2017
 * Modified: 8/12/2017, added support for geometry types and reprojection using GeoTools instead of string manipulation of WKT literals
 * Modified: 12/12/2017, fixed issue with string encodings; verified that UTF characters read and written correctly
 * Last modified: 8/3/2018
 */
public class RdfToShp implements ReverseConverter {

	  BatchReverseConverter myReverseConverter;
	  Assistant myAssistant;
	  private MathTransform reproject = null;
	  WKTReader reader;
	  int sourceSRID;                                  //Source CRS according to EPSG 
	  int targetSRID;                                  //Target CRS according to EPSG
	  private ReverseConfiguration currentConfig;      //User-specified configuration settings
	  private String outputFile;                       //Output shapefile
	  private String encoding;                         //Encoding of the data records
	  
	  ShapefileDataStore newDataStore;
	  SimpleFeatureBuilder featureBuilder;
	  SimpleFeatureCollection collection;
	  String geomType;
	  String typeName;
	  String attrGeom;                                 //Name of the geometry attribute in the resulting file
	  SimpleFeatureSource featureSource;
	  SimpleFeatureStore featureStore;
	  Transaction transaction;
	  int numRecs;
	  
	  //Initialize a CRS factory for possible reprojections
	  private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
		       .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
		

	  /**
	   * Constructor for the reverse transformation process from RDF to ESRI shapefile.
	   * @param config  Parameters for Reverse Transformation.
	   * @param outFile   Path to the output shapefile.
	   * @param sourceSRID  Spatial reference system (EPSG code) of geometries in the input RDF triples
	   * @param targetSRID  Spatial reference system (EPSG code) of the output shapefile
	   */
	  public RdfToShp(ReverseConfiguration config, String outFile, int sourceSRID, int targetSRID) {
       
		  myAssistant = new Assistant();
		  currentConfig = config;
		  outputFile = outFile;
	      this.sourceSRID = sourceSRID;
	      this.targetSRID = targetSRID;
	      encoding = config.encoding;        //User-specified encoding; is not specified, UTF-8 is assumed
		  
	      //Create a shapefile data store for accommodating all resulting records
	      //Schema and spatial reference system defined later, once the first record arrives
	      try {
	    	  File shp = new File(outputFile);	    	  
	          ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
	          Map<String, Serializable> params = new HashMap<String, Serializable>();
	          params.put("url", shp.toURI().toURL());
	          params.put("create spatial index", Boolean.FALSE);
	          params.put("charset", encoding);
	          newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
      
	          //This collection will hold the batch of results that will be written into the shapefile
	          collection = FeatureCollections.newCollection();
	          numRecs = 0;
	          
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
			  transaction.commit();
              transaction.close();
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
		        	Literal r = sol.getLiteral(attrList.get(i));	
		        	if (r != null)
		        	{   //Designating typical data types for attributes
		        		String d = r.getDatatypeURI().toLowerCase();
		        		if (d.contains("wktliteral"))        //ASSUMPTION: GeoSPARQL WKT literals always include their data type specification
		        		{
		        			attrGeom = attrList.get(i);      //Single out the name of the geometry attribute
		        			typeSpec += attrList.get(i) + getGeometrySpec(r);   
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
		        
		        //Data type specification for the shapefile
		        SimpleFeatureType featTypes = DataUtilities.createType("ShapefileSchema", typeSpec);
			    newDataStore.createSchema(featTypes);
			    featureBuilder = new SimpleFeatureBuilder(featTypes);
			    typeName = newDataStore.getTypeNames()[0];               //This is called "ShapefileSchema"
			    
			    //The feature source representing the new shapefile
			    featureSource = newDataStore.getFeatureSource(typeName);
				featureStore = (SimpleFeatureStore) featureSource;
				transaction = new DefaultTransaction("create");
				  
//			    System.out.println(featTypes.toString());  
			      
			    return true;                 //Schema has been created successfully
			
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Failed to initialize the output file.");
	  	  }
	      
	      return false;      
	  }

	  
	  /**
	   * Identifies SRID (EPSG code) and geometry type from a WKT literal.
	   * @param g  A geometry WKT literal.
	   * @return  A string that concatenates the geometry type and the SRID of the given WKT literal.
	   */
	  public String getGeometrySpec(Literal g)
	  	{  	
	  		int i = g.getLexicalForm().indexOf('>');
	  		String epsg = g.getLexicalForm().substring(g.getLexicalForm().lastIndexOf("EPSG/0/")+7, i).trim();
	  		
	  		//First identify the SRID in order to specify the projection of the resulting shapefile
	  		try {
				if ((epsg != "") && (sourceSRID != Integer.parseInt(epsg)))
				{
					try {
						throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
					} catch (Exception e) {
						ExceptionHandler.abort(e, "ERROR: Input geometries are not georeferenced in the CRS specified in the configuration!");
					}
				}
				else if ((epsg != "") && (myAssistant.isNullOrEmpty(currentConfig.targetCRS)))  //Geometries specify SRID and no reprojection is required
	  				newDataStore.forceSchemaCRS(CRS.decode("EPSG:" + epsg));
	  			else if (targetSRID != 4326)                                    //Reprojection is required, so this must be specified in the shapefile
		  		{
		  			epsg = "" + targetSRID;   
		  			newDataStore.forceSchemaCRS(CRS.decode("EPSG:" + epsg));		
		  		}
	  			else if ((targetSRID == 4326) || (epsg == ""))                  //Use WGS1984 as SRID if no spatial reference system is specified for geometries
		  		{
		  			epsg = "4326";   
		  			newDataStore.forceSchemaCRS(DefaultGeographicCRS.WGS84);		
		  		}
		  			
	  		} catch (Exception e) {
	  			ExceptionHandler.abort(e, "ERROR: Spatial reference (CRS) for geometries cannot be specified!");
			}
	  		
	  		//Also identify the geometry type, as specified immediately before the list of coordinates
	  		//CAUTION: Shapefiles are case-sensitive in all definitions (including geometry types)
	  		geomType = g.getLexicalForm().substring(i+1, g.getLexicalForm().indexOf("(")).trim();
	  		geomType = geomType.substring(0, 1).toUpperCase() + geomType.substring(1).toLowerCase();
	  		try {
		  		if ((geomType.toUpperCase().equals("POLYGON") || geomType.toUpperCase().equals("MULTIPOLYGON")))
		  			geomType = "MultiPolygon";
		  		else if ((geomType.toUpperCase().equals("LINESTRING")) || (geomType.toUpperCase().equals("MULTILINESTRING")))
		  			geomType = "MultiLineString";
		  		else if ((!geomType.toUpperCase().equals("POINT")) && (!geomType.toUpperCase().equals("MULTIPOINT")))
		  			throw new UnsupportedOperationException("Input geometries are of type " + geomType);
	  		} catch (Exception e) {
            	ExceptionHandler.abort(e, "ERROR: Geometry type " + geomType + " is not supported in shapefiles.");
            }
	  		
	  		System.out.println("All geometries are expected to be of type " + geomType.toUpperCase() + " in order to be included in this shapefile.");
	  		
	  		return ":" + geomType + ":srid=" + epsg; 
	  	}
	  	  
	  
	/**
	 * Creates a feature collection from current batch of records and writes them into the output shapefile.
	 * @param attrList  List of attribute names.
	 * @param results  Collection of records in the current batch.
	 * @return  The number of records resulted in the current batch.  
	 */
	@SuppressWarnings("static-access")
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
			    		{   //Input geometry is in EWKT, so SRID specification must be stripped off and converted to WKT
			    			Geometry g;
			    			if (reproject != null)                //Apply on-the-fly reprojection, if specified
			    				g = myAssistant.WKT2GeometryTransform(rec.get(i).substring(rec.get(i).indexOf('>')+1).trim(), reproject, reader);
			    			else
			    				g = myAssistant.WKT2Geometry(rec.get(i).substring(rec.get(i).indexOf('>')+1).trim());
			    			if (!g.isValid() || (!geomType.contains(g.getGeometryType())))
			    			{
			    				System.out.println("Geometry is not valid or has a different type with the rest in the shapefile.");
			    				featureBuilder.reset();
			    				valid = false;    //Mark this geometry as invalid; Also applies to a valid WKT, but incompatible with the geometry type specified for the shapefile
			    				break;
			    			}
			    			else
			    				featureBuilder.add(g);    //Valid geometry, also compatible with the geometry type in the shapefile
			    		}
			    		else 
			    			featureBuilder.add(rec.get(i));
			    	}

			    	if (valid)                 //Only valid geometries should be included in the shapefile
			    	{
			    		SimpleFeature feature = featureBuilder.buildFeature(null);
			    		collection.add(feature);	
			    		numRecs++;
			    	}
			    }
			    
//			    System.out.println("Collection with " + collection.size() + " features created!");
			    		
                //Write the collected features into the shapefile
                try {
                    if (featureSource instanceof SimpleFeatureStore) {
                        featureStore.setTransaction(transaction);
                        featureStore.addFeatures(collection);
                        collection = FeatureCollections.newCollection();      //Create a new collection for the next batch
                    } 
                } catch (Exception e) {
                	ExceptionHandler.abort(e, "ERROR: This " + typeName + " does not support read/write access.");
                    transaction.rollback();
                }                    
			    				
	      } catch (Exception e) {
	    	  ExceptionHandler.abort(e, "Failed to store records into the output file.");
	  	  }
	      
	      return numRecs;
	}


	@Override
	public String getGeometrySpec(String g) {
		return null;
	}
	  
}
