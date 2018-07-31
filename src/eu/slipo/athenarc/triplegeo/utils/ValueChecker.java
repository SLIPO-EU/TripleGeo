/*
 * @(#) ValueChecker.java 	 version 1.5   31/7/2018
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

import java.util.HashMap;
import java.util.Map;

/**
 * Removes or replaces illegal characters from a literal value.
 * LIMITATIONS: Currently handling only some basic cases that may cause trouble (e.g., line breaks) in resulting files with RDF triples.
 * @author Kostas Patroumpas
 * @version 1.5
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 27/7/2018
 * Modified: 27/7/2018; replaced any appearance of the delimiter character in string values
 * Last modified: 31/7/2018
 */

public class ValueChecker {

	private Map<String, String> replacements;  //List of string values to check (keys) for presence in a given literal and their respective replacements (values)
	           
    /**
     * Constructs a ValueChecker object that will be used for checking (and possibly correcting) literals for specific anomalies before issuing RDF triples
     */
	public ValueChecker() {
		 
		//TODO: Utilize an external resource file where correspondence between unwanted characters and their replacements can be specified by the user.
		replacements = new HashMap<String, String>();
		replacements.put(Constants.REGISTRY_CSV_DELIMITER, " ");     //Replace predefined delimiter with a BLANK character
		replacements.put("|",""); 
		replacements.put("\\s+",""); 
		replacements.put("[\\\t|\\\n|\\\r]"," "); 
	    
	 }

	/**
	 * Eliminates illegal characters from the given string value (literal)
	 * @param val  A string value.
	 * @return  The same string value with any illegal characters removed.
	 */
	 public String removeIllegalChars(String val) {
		  if (val != null)
			  return val.replaceAll("[\\\t|\\\n|\\\r]"," ");     //FIXME: Replacing newlines with space, but other special characters should be handled as well
		  return "";                                             //In case of NULL values, return and empty string
	  }
	
	  /**
	   * Eliminates any appearance of the delimiter character in the given string value (literal)
	   * @param val  A string value.
	   * @return  The same string value after the delimiter character has been replaced. 
	   */
	  public String removeDelimiter(String val) {
		  if (val != null)
			  return val.replace(Constants.REGISTRY_CSV_DELIMITER, " ");     //Replace delimiter with a BLANK
		  return "";                                                         //In case of NULL values, return and empty string
	  }
		
	  /**
	   * Restores the given string value as a URL
	   * @param val   A string value representing a URL
	   * @return  The same string value as a valid HTTP address
	   */
	  public String cleanupURL(String val) {
		  if (val != null) {
			  val = val.replaceAll("\\s+","");                  //Eliminate white spaces and invalid characters
			  val = val.replaceAll("|","");                     //Character | is invalid for URLs
			  val = val.replace("\\","/");                      //Backslash characters '\' are not allowed in URLs
			  if (!val.toLowerCase().matches("^\\w+://.*"))     //This value should be a URL, so put HTTP as its prefix
				  val = "http://" + val;                        //In case that no protocol has been specified, assume that this is HTTP
		  }
		  return val;
	  }
	  
	  /**
	   * Remove whitespace characters from a URL
	   * @param val  A string value representing a URL
	   * @return  The URL with any whitespace characters replaced
	   */
	  public String replaceWhiteSpaceURL(String val) {
		  if (val != null)
			  return val.replace(Constants.WHITESPACE, Constants.REPLACEMENT);
		  return val;
	  }
}
