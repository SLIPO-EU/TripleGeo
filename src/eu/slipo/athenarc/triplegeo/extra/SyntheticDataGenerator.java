/*
 * @(#) SyntheticDataGenerator.java	version 2.0   31/10/2019
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
package eu.slipo.athenarc.triplegeo.extra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.util.AffineTransformation;
import com.vividsolutions.jts.io.WKTReader;

import eu.slipo.athenarc.triplegeo.utils.Assistant;
import eu.slipo.athenarc.triplegeo.utils.ExceptionHandler;

/**
 * Auxiliary utility that creates a synthetic dataset based on a given CSV dataset by translating geometries, modifying names, and randomly erasing attribute values. 
 * It can be used to create synthetic POI data, by inflating and modifying a POI dataset given as seed.
 * IMPORTANT! This utility does NOT apply over RDF data; it handles CSV files only that may be accepted by TripleGeo for transformation to RDF.
 * LIMITATION: Most parameters for the synthetic data generation are hard-coded (referring to OSM-based input data). Please modify and recompile, if necessary.
 * USAGE: Execution command over JVM:
 *           java -cp target/triplegeo-2.0-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.extra.SyntheticDataGenerator <path-to-input-CSV-file> <dX> <dY> <part>
 * ARGUMENTS: (1) Path to input CSV file containing the spatial entities and their thematics attribute values.
 *            (2) Displacement <dX> of the geometry on the x-axis (longitude).
 *            (3) Displacement <dY> of the geometry on the y-axis (latitude).
 *            (4) User-defined string that will suffix each generated identifier.
 * 
 * @author Kostas Patroumpas
 * @version 2.0
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 6/2/2019
 * Modified: 8/2/2019; randomly choosing the attribute value that will be erased
 * Modified: 11/2/2019; included transliteration of the name attribute
 * Last modified: 31/10/2019
 */
public class SyntheticDataGenerator {

	//CAUTION! Hard-coded parameters related to the input file; Column names are case-sensitive!
	private static char DELIMITER = '|';               //Delimiter character for attribute values in the CSV files (both input and output)
	private static char QUOTE = '"';                   //Quote character used for string literals
	private static int srid = 4326;                    //SRID of the dataset
	private static String colID = "osm_id";            //Identifier column in the input CSV file
    private static String colGeom = "wkt";             //Geometry column in the input CSV file
    private static String colLon = "lon";              //Longitude column in the input CSV file
    private static String colLat = "lat";              //Latitude column in the input CSV file
    private static String colName = "name";            //Name column in the input CSV file
    private static String colTranslit = "translit";    //Extra column that will hold the transliterated name (only appears in the output CSV file)
    private static String[] colKeep = {"osm_id","category","type"};   //Names of columns that must NOT be altered
	
    private static String encoding;                    //Encoding of the data records
	private static String[] csvHeader = null;          //CSV Header
	private static CSVFormat csvFormat = null;         //CSV format      
	private static double dx = 0.0;                    //Displacement for translation over the x-axis
	private static double dy = 0.0;                    //Displacement for translation over the y-axis	
	private static String part ="";                    //User-defined suffix to the identifiers of the generated features
	static Assistant myAssistant =  new Assistant();
	  
	/**
	 * Loads the CSV file from the configuration path and returns an iterable feature collection.  
	 * @param filePath  The path to the CSV file containing data features.
	 * @return  Iterator over the collection of CSV records that can be streamed into the transformation process.
	 */
	@SuppressWarnings("resource")
	private static Iterator<CSVRecord> collectData(String filePath) {
	  
		Iterator<CSVRecord> records = null;
		try {
			File file = new File(filePath);
		
			//Check for several UTF encodings and change the default one, if necessary
			BOMInputStream bomIn = new BOMInputStream(new FileInputStream(file), ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE);
			if (bomIn.hasBOM(ByteOrderMark.UTF_8)) 
				encoding = StandardCharsets.UTF_8.name();
			else if (bomIn.hasBOM(ByteOrderMark.UTF_16LE)) 
				encoding = StandardCharsets.UTF_16LE.name();
			else if (bomIn.hasBOM(ByteOrderMark.UTF_16BE))
				encoding = StandardCharsets.UTF_16BE.name();
			else
				encoding = StandardCharsets.UTF_8.name();         //Otherwise, assume standard UTF-8 encoding
		
			System.out.println("Encoding:" + encoding);
			
			//Read records and header from the input file
			Reader in = new InputStreamReader(new FileInputStream(file), encoding);
			csvFormat = CSVFormat.RFC4180.withDelimiter(DELIMITER).withQuote(QUOTE).withFirstRecordAsHeader();					
			CSVParser dataCSVParser = new CSVParser(in, csvFormat);
			records = dataCSVParser.iterator();                                     //List of all records
			
			//Hold an array of all column names
			//CAUTION! An extra attribute will be used to hold the transliterated name, if it not already exists
			Map<String, Integer> headerMap = dataCSVParser.getHeaderMap();
			if (!headerMap.containsKey(colTranslit))
				headerMap.put(colTranslit, dataCSVParser.getHeaderMap().size()+1);
			csvHeader = headerMap.keySet().toArray(new String[headerMap.size()]);
			
		} catch (Exception e) {
			ExceptionHandler.abort(e, "Cannot access input file.");      //Execution terminated abnormally
		}
		
		return records;
	}

	/**
	 * Pick a random position within a range [min..max]
	 * @param min  The lower bound of the range.
	 * @param max  The upper bound of the range.
	 * @return  The randomly chosen position in the range.
	 */
	private static int pickRandomPos(int min, int max) {
		return min + (int)(Math.random() * ((max - min) + 1));    
	}

	
	/**
	 * Pick a random value from an array of integers.
	 * @param objects  An array of integer values.
	 * @return  The randomly chosen integer from the array.
	 */
	public static int pickRandomInt(Integer[] objects) {
	    int rnd = new Random().nextInt(objects.length);
	    return objects[rnd];
    }

	/**
	 * Flip a coin and determine a boolean value.
	 * @return  The boolean value.
	 */
	private static boolean tossCoin(){
	    int result = new Random().nextInt(2);
	    if (result == 0)
	        return true;
	    return false;
	}

	/**
	 * Creates a synthetic dataset from an input CSV file containing spatial entities and their thematic attributes.
	 * @param inFile  The input CSV file with the spatial entities.
	 * @param outFile  The output CSV file with the spatial entities having their attributes (spatial and thematic) modified in a random fashion.
	 * @throws Exception
	 */
	public static void modifyCSVFile(String inFile, String outFile) throws Exception {

		try {
            //Collect results from the CSV file
			Iterator<CSVRecord> records = collectData(inFile);
			
        	//Use the same specifications for the output dataset
            CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(new FileOutputStream(outFile), encoding), csvFormat);
            
            WKTReader wktReader = new WKTReader();             //Parses a geometry in Well-Known Text format to a Geometry representation
            Geometry geom = null;
            Geometry transGeom = null;

            int n = 0;
            int rejected = 0;
                 
            //Specify geometric translation with a certain displacement by dX, dY in the same projection as the input
            AffineTransformation at = new AffineTransformation();
			at.translate(dx, dy);
		
			//Identify the column containing the geometry (WKT)
			int i = 0;
			int iID = 0;
			int iWKT = 0;
			int iLon = 0;
			int iLat = 0;
			int iName = 0;
			int iTranslit = 0;
			int numModifiedNames = 0;
			int numErasedAttrs = 0;
			
			//Keep rest of the attributes in a list
			List<Integer> listRestAttrs = new ArrayList<Integer>();
			
			for (String col: csvHeader)
			{
				if (col.equals(colGeom))
					iWKT = i;
				else if (col.equals(colID))
					iID = i;
				else if (col.equals(colLon))
					iLon = i;
				else if (col.equals(colLat))
					iLat = i;
				else if (col.equals(colName))
					iName = i;
				else if (col.equals(colTranslit))
					iTranslit = i;
				else if (!Arrays.asList(colKeep).contains(col))
					listRestAttrs.add(i);
				i++;
			}
			
			//Array of attributes that are candidates for elimination
			Integer[] candAttrs = (Integer[])listRestAttrs.toArray(new Integer[listRestAttrs.size()]);
			
			//OPTIONAL: Replicate header with column names in the beginning of the output file
			print(printer, csvHeader);
			
			//Iterate through all records
			for (Iterator<CSVRecord> iterator = records; iterator.hasNext();) 
			{		
	            CSVRecord rs = (CSVRecord) iterator.next();
	            String[] s = CSV2Array(rs);
	            
		      	//STAGE I: Handle geometry value
	            String wkt = s[iWKT];                             //ASSUMPTION: Geometry values are given as WKT  
				
		      	if (wkt != null)
		      	{	
		            //Apply transformation
		            try {
		            	geom = wktReader.read(wkt);
		            	geom.setSRID(srid);	            	
		            	transGeom = at.transform(geom);	
		            	
		            	//Check whether transformed geometry exceeds bounds of the given SRID
		            	if ( (Math.abs(transGeom.getCentroid().getX()) > 179) || (Math.abs(transGeom.getCentroid().getY()) > 89))
                        {
		            		System.out.println("Transformed geometry NOT in WGS84 bounds:" + transGeom.getCentroid().getX() + "," + transGeom.getCentroid().getY() + " DISTANCE: " + geom.distance(transGeom));	 	            	
                            rejected++;
                            continue;
		            	}
	
		            	s[iWKT] = transGeom.toText();                                          //Update WKT of the translated geometry
		            	s[iLon] = String.valueOf(transGeom.getCentroid().getX());              //Update Longitude of the translated geometry
		            	s[iLat] = String.valueOf(transGeom.getCentroid().getY());              //Update Longitude of the translated geometry
		    		} catch (Exception e) {
		    			e.printStackTrace();
		    		}
		      	}	
	      	
		      	//STAGE II: Assign a new id to the feature, simply by suffixing it with a serial number
		      	s[iID] = s[iID] + "_" + part + n;  
		      	
		      	//STAGE III: Transliterate the name value
		      	s[iTranslit] = myAssistant.getTransliteration(s[iName]);
//		      	System.out.println("Original name:" + s[iName] + " transliterated to " + s[iTranslit]);		      	
		      	
		      	//STAGE IV: Randomly modify the string value of TRANSLITERATED name
		      	if (tossCoin())    //Flip a coin to decide whether to apply this stage
		      	{
			      	StringBuilder aName = new StringBuilder(s[iTranslit]);
			      	if ((aName != null) && (aName.length() > 5))                   //...provided that the string is at least 5 characters long
			      	{
			      		//Eliminate a random character within this string
			      		aName.deleteCharAt(pickRandomPos(1, aName.length()-1));
			      		s[iTranslit] = aName.toString();
			      		numModifiedNames++;
			      	}
		      	}
		      	
		      	//STAGE V: Randomly pick any other attribute and erase its value 
		      	if (candAttrs.length > 1) 
		      	{
		      		int j = pickRandomInt(candAttrs);
		      		if (!s[j].equals(""))
		      		{
		      			s[j] = "";
		      			numErasedAttrs++;
		      		}		      		
		      	}
		    
                //Notify progress
		      	n++;
         		if (n % 1000 == 0)
				System.out.print(n + " features processed..." + "\r");  	
		
	            //Copy transformed record to output file
	            print(printer, s);
			}
			
			//Close output file
	        printer.close();

	        System.out.println("CSV file with " + n + " records created successfully! " + rejected + " records were skipped becaused of out-of-bound geometries. " + numModifiedNames + " names were modified. " + numErasedAttrs + " attribute values were randomly erased.");
	        System.out.println("Output data written to file:" + outFile);
        } catch (Exception e) {
			e.printStackTrace();
		}
		
    }

 

	/**
	 * Convert a CSV record as array for manipulating its constituent values.
	 * @param rec  A CSV record with attribute values
	 * @return  The array of same values.
	 */
	public static String[] CSV2Array(CSVRecord rec) {
            String[] arr = new String[csvHeader.length];    //CAUTION! An extra attribute may have been added to the end; this will hold the transliterated name value, if it did not exist
            int i = 0;
            for (String str : rec) 
                arr[i++] = str;

            return arr;
        }


	/**
	 * Print array of values as a record to output CSV file.
	 * @param printer   The CSV printer to be used for issuing results to the output file.
	 * @param s  An array of attribute values to write as a new record in the file.
	 * @throws Exception
	 */
    public static void print(CSVPrinter printer, String[] s) throws Exception {
        for (String val : s)
            printer.print(val != null ? String.valueOf(val) : "");
            
        printer.println();
    }
    
	
	public static void main(String[] args) {

		if (args.length < 4)
		{
			System.err.println("Wrong parametrization. Usage: <INPUT CSV file> <dX> <dY> <part>");
			System.exit(1);          //Execution terminated abnormally
		}
		
		String inFile = args[0];
			
        //Accept user-specified values for geometric translation
        dx = Double.parseDouble(args[1]);
        dy = Double.parseDouble(args[2]);
        
        //User-defined string that will suffix each generated identifier
        part = args[3];
        
        //The name of the output CSV file is the same as input suffixed with the displacement parameters
        String outFile = inFile.substring(0, inFile.length()-4) + "dx" + dx + "dy" + + dy + ".csv";

        try {
			modifyCSVFile(inFile, outFile);
		} catch (Exception e) {
			e.printStackTrace();
		}       
	}

}

