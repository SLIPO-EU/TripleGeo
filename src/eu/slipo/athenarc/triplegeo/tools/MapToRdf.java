/*
 * @(#) MapToRdf.java 	 version 1.8   7/3/2019
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

import eu.slipo.athenarc.triplegeo.utils.*;

import org.apache.commons.io.FilenameUtils;
import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import java.util.Iterator;
import java.util.Map;

/**
 * Main entry point of the utility for extracting RDF triples from a Map.
 * @author Georgios Mandilaras
 * @version 1.8
 */

/* DEVELOPMENT HISTORY
 * Created by: Georgios Mandilaras, 20/12/2018
 * Last modified: 7/3/2019
*/
public class MapToRdf {

    Converter myConverter;
    Assistant myAssistant;
    private MathTransform reproject = null;
    int sourceSRID;                            //Source CRS according to EPSG
    int targetSRID;                            //Target CRS according to EPSG
    private Configuration currentConfig;       //User-specified configuration settings
    private Classification classification;     //Classification hierarchy for assigning categories to features
    String outputFile;                         //Output RDF file

    private Iterator<Map<String,String>> data;
    private int partition_index;


    //Initialize a CRS factory for possible reprojections
    private static final CRSAuthorityFactory crsFactory = ReferencingFactoryFinder
            .getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));

    public MapToRdf(Configuration config, Classification classific, String outFile, int sourceSRID, int targetSRID, Iterator<Map<String,String>> input, int index) throws ClassNotFoundException {

        this.currentConfig = config;      
        myAssistant = new Assistant();
        
		//Check whether a classification hierarchy is specified in a separate file and apply transformation accordingly
        try { 			  
			if ((currentConfig.classificationSpec != null) && (!currentConfig.inputFormat.contains("OSM")))    //Classification for OSM XML data is handled by the OSM converter
			{
				String outClassificationFile = currentConfig.outputDir + FilenameUtils.getBaseName(currentConfig.classificationSpec) + "_" + index + myAssistant.getOutputExtension(currentConfig.serialization);
				this.classification = new Classification(currentConfig, currentConfig.classificationSpec, outClassificationFile);  
			} 
        }  
     	//Handle any errors that may have occurred during reading of classification hierarchy  
        catch (Exception e) {  
        	ExceptionHandler.abort(e, "Failed to read classification hierarchy.");  
     	} 
        finally {
        	if (this.classification != null)
        		System.out.println(myAssistant.getGMTime() + " Classification hierarchy read successfully!");
        	else if (!currentConfig.inputFormat.contains("OSM"))
        		System.out.println("No classification hierarchy specified for features to be extracted.");
        }
        
        this.outputFile = outFile;
        this.sourceSRID = sourceSRID;
        this.targetSRID = targetSRID;        
        this.data = input;
        this.partition_index = index;
        //Check if a coordinate transform is required for geometries
        if (currentConfig.targetCRS != null) {
            try {
                boolean lenient = true; // allow for some error due to different datums
                CoordinateReferenceSystem sourceCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.sourceCRS);
                CoordinateReferenceSystem targetCRS = crsFactory.createCoordinateReferenceSystem(currentConfig.targetCRS);
                reproject = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            } catch (Exception e) {
                ExceptionHandler.abort(e, "Error in CRS transformation (reprojection) of geometries.");      //Execution terminated abnormally
            }
        } else  //No transformation specified; determine the CRS of geometries
        {
            if (sourceSRID == 0)
                this.targetSRID = 4326;          //All features assumed in WGS84 lon/lat coordinates
            else
                this.targetSRID = sourceSRID;    //Retain original CRS
        }
        // Other parameters
        if (myAssistant.isNullOrEmpty(currentConfig.defaultLang)) {
            currentConfig.defaultLang = "en";
        }
    }


    /*
     * Applies transformation according to the configuration settings.
     */
    public void apply() {
        try {
            if (currentConfig.mode.contains("STREAM")) {
                //Mode STREAM: consume records and streamline them into a serialization file
                myConverter = new StreamConverter(currentConfig, outputFile);
                while (data.hasNext()) {
                    String wkt = null;
                    String geomType = null;
                    Map<String, String> map = data.next();
                    if (currentConfig.inputFormat.equals("SHAPEFILE") || currentConfig.inputFormat.equals("GEOJSON")){
                        wkt = map.get("wkt_geometry");
                        map.remove("wkt_geometry");
                        geomType = wkt.split(" ")[0];
                    }
                    //Export data in a streaming fashion
                    myConverter.parse(myAssistant, wkt, map, classification, targetSRID, reproject, geomType, partition_index, outputFile);
                }
                //Store results to file
                myConverter.store(myAssistant, outputFile, partition_index);
            }
        } catch (Exception e) {
            ExceptionHandler.abort(e, "");
        }
    }
}
