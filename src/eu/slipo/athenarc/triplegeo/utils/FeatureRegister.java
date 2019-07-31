/*
 * @(#) FeatureRegister.java 	 version 1.9   27/7/2018
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Creates a record of attribute values concerning a feature that should be registered in the SLIPO Registry.
 * @author Kostas Patroumpas
 * @version 1.9
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 24/1/2018
 * Modified: 24/1/2018, added export of basic attributes for SLIPO Registry
 * Modified: 12/2/2018, added on-the-fly calculation of lon/lat coordinates for SLIPO Registry
 * Modified: 3/7/2018; replaced any appearance of the delimiter character in string values
 * Last modified: 27/7/2018
 */

public class FeatureRegister {

	private static Configuration currentConfig;
	
	Assistant myAssistant;
	ValueChecker myChecker;
	
	private String rTuple;                 //Contains attribute values that will be used for registering features in the SLIPO Registry
	private List<String> tuples4Registry;  //List of tuples for the SLIPO Registry as collected after transforming a batch of input features
	
	private List<String> attrKeys;         //Original names of the attributes to register             
    /**
     * Constructs a FeatureRegister object that will be used for collecting feature attributes to be registered in the SLIPO Registry.
     * @param config  User-specified configuration for the transformation process.
     */
	public FeatureRegister(Configuration config) {
		    
	    currentConfig = config;       //Configuration parameters as set up by the various conversion utilities (CSV, SHP, DBMS, etc.) 
	    
	    myAssistant = new Assistant(config);
	    myChecker = new ValueChecker();
	    
	    attrKeys = new ArrayList<String>();   //Collection of names of non-spatial (thematic) attributes to take part in the registration
	    
	    tuples4Registry = new ArrayList<>();  //Hold a collection of tuples for the SLIPO Registry
	    
	 }

	/**
	 * Adds a column name in the collection of non-spatial (thematic) attributes to be used in creating the records for registration
	 * @param key  The name of an attribute as specified in the input dataset
	 * @return  True if the attribute has been included; otherwise, False.
	 */
	public boolean includeAttribute(String key) {
		
		return attrKeys.add(key);
	}
	
	  /**
	   * Provides a collection of custom tuples with attribute values that will be used for the SLIPO Registry
	   * @return  A list of records (their attributes typically separated with a delimiter character)
	   */
	  public List<String> getTuples4Registry() {
		  
		  return tuples4Registry;
	  }


	  /**
	   * Cleans up all tuples collected for the SLIPO Registry so far
	   */
	  public void clearTuples4Registry() {
		  
		  tuples4Registry.clear();	  
	  }
	  
	  
	  /**
	   * Creates a record of attributes (as a .CSV record) for a feature to be registered in the SLIPO Registry.
	   * @param uri  The URI assigned to the feature
	   * @param row  Attribute values for each thematic (non-spatial) attribute
	   * @param wkt  Well-Known Text representation of the geometry  
	   * @param targetSRID  The EPSG identifier of the Coordinate Reference System of the geometry
	   */
	  public void createTuple(String uri, Map<String,String> row, String wkt, int targetSRID) {

		try {
			rTuple = null;
			double coords[] = null;

	      	//Export selected attributes as required for SLIPO Registry
  	        rTuple = uri + Constants.REGISTRY_CSV_DELIMITER + myChecker.removeDelimiter(currentConfig.featureSource);
  	        for (String key: attrKeys)
  	        {
  	        	rTuple += Constants.REGISTRY_CSV_DELIMITER + myChecker.removeDelimiter(row.get(key));
  	        }

  	        //Include lon/lat coordinates at WGS84 even if a geometry WKT is georeferenced in another SRID
  	        if (wkt != null) 
  	        { 
  	        	coords = myAssistant.getLonLatCoords(wkt, targetSRID);
	  	        if (coords != null)
	  	        	rTuple += Constants.REGISTRY_CSV_DELIMITER + coords[0] + Constants.REGISTRY_CSV_DELIMITER + coords[1];
	  	        else
	  	        	rTuple += Constants.REGISTRY_CSV_DELIMITER + Constants.REGISTRY_CSV_DELIMITER;
  	        }
  	        tuples4Registry.add(rTuple);		
		}
		catch(Exception e) { 
			ExceptionHandler.warn(e, "An error occurred during transformation of an input record.");
		}
			
	  }
		
}
