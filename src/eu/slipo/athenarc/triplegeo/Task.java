/*
 * @(#) Executor.java	version 1.3   28/11/2017
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
package eu.slipo.athenarc.triplegeo;

import eu.slipo.athenarc.triplegeo.tools.CsvToRdf;
import eu.slipo.athenarc.triplegeo.tools.GeoJsonToRdf;
import eu.slipo.athenarc.triplegeo.tools.GpxToRdf;
import eu.slipo.athenarc.triplegeo.tools.RdbToRdf;
import eu.slipo.athenarc.triplegeo.tools.ShpToRdf;
import eu.slipo.athenarc.triplegeo.tools.OsmToRdf;
import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.Classification;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * Running a transformation task under the given configuration settings
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 23/3/2017
 * Modified by: Kostas Patroumpas, 18/10/2017
 * Modified: 8/11/2017, added support for system exit codes on abnormal termination
 * Modified: 21/11/2017, added support for user-specified classification schemes for shapefiles, CSV, and DBMS data sources 
 * Last modified by: Kostas Patroumpas, 28/11/2017
 */
public class Task {

	private String currentFormat;	
	Assistant myAssistant;

	public Task(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) {

    	currentFormat = config.inputFormat.toUpperCase();           //Possible values: SHAPEFILE, DBMS, CSV, GPX, GEOJSON, OSM, XML
    	//System.out.println("Transforming " + inFile + " from " + currentFormat + " into " + outFile);
    	
        try {		
			//Apply data transformation according to the given input format
			if (currentFormat.trim().contains("SHAPEFILE")) {
				ShpToRdf conv = new ShpToRdf(config, classific, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("DBMS")) {
				RdbToRdf conv = new RdbToRdf(config, classific, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("CSV")) {			
				CsvToRdf conv = new CsvToRdf(config, classific, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("GPX")) {
				GpxToRdf conv = new GpxToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("GEOJSON")) {
				GeoJsonToRdf conv = new GeoJsonToRdf(config, classific, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("OSM")) {
				OsmToRdf conv = new OsmToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("XML")) {   //This includes INSPIRE data (GML) and metadata (XML), as well as GML and KML files
				String fileXSLT = config.mappingSpec;          //Predefined XSLT stylesheet to be applied in transformation
				myAssistant =  new Assistant();
				myAssistant.saxonTransform(inFile, fileXSLT, outFile);
			}	
			else {
				throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
			}
				
        } catch (Exception e) {
        	ExceptionHandler.invoke(e, Constants.INCORRECT_SETTING);      //Execution terminated abnormally
		}
	}   

}
