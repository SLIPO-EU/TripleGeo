/*
 * @(#) Executor.java	version 1.2   26/7/2017
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
package eu.slipo.athenarc.triplegeo;

import eu.slipo.athenarc.triplegeo.tools.CsvToRdf;
import eu.slipo.athenarc.triplegeo.tools.GeoJsonToRdf;
import eu.slipo.athenarc.triplegeo.tools.GpxToRdf;
import eu.slipo.athenarc.triplegeo.tools.RdbToRdf;
import eu.slipo.athenarc.triplegeo.tools.ShpToRdf;
import eu.slipo.athenarc.triplegeo.tools.OsmToRdf;
import eu.slipo.athenarc.triplegeo.utils.Configuration;
import eu.slipo.athenarc.triplegeo.utils.Constants;

/**
 * Running a transformation task under the given configuration settings
 * @author Kostas Patroumpas
 * Created by: Kostas Patroumpas, 23/3/2017
 * Last modified by: Kostas Patroumpas, 26/7/2017
 */
public class Task {

	private String currentFormat;	
	
	public Task(Configuration config, String inFile, String outFile, int sourceSRID, int targetSRID) {

    	currentFormat = config.inputFormat.toUpperCase();           //Possible values: SHAPEFILE, DBMS, CSV, GPX, GEOJSON
    	//System.out.println("Transforming " + inFile + " from " + currentFormat + " into " + outFile);
    	
        try {		
			//Apply data transformation according to the given input format
			if (currentFormat.trim().contains("SHAPEFILE")) {
				ShpToRdf conv = new ShpToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("DBMS")) {
				RdbToRdf conv = new RdbToRdf(config, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("CSV")) {			
				CsvToRdf conv = new CsvToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("GPX")) {
				GpxToRdf conv = new GpxToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("GEOJSON")) {
				GeoJsonToRdf conv = new GeoJsonToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else if (currentFormat.trim().contains("OSM")) {
				OsmToRdf conv = new OsmToRdf(config, inFile, outFile, sourceSRID, targetSRID);
				conv.apply();
			}
			else
			{
				System.err.println(Constants.INCORRECT_SETTING);
			}			
        } catch (Exception e) {
			e.printStackTrace();
		}
	}   

}
