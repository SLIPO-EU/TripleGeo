/*
 * @(#) ShpPartitioner.java	version 2.0  22/4/2019
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
package eu.slipo.athenarc.triplegeo.partitioning;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import eu.slipo.athenarc.triplegeo.utils.Assistant;


/**
 * Partitions a ESRI shapefile into several (equi-sized) parts in order to be concurrently transformed into RDF.
 * @author Kostas Patroumpas
 * @version 2.0
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 4/10/2018
 * Last modified: 22/4/2019
 */

public class ShpPartitioner implements Partitioner {

	String encoding = "UTF-8";      //Default encoding of the input shapefile
	String fileExtension = ".shp";  //Default extension for shapefiles
	List<String> outputFiles;       //List to hold the paths to all partitions
	
	//Constructor of this class
	public ShpPartitioner() { 
		outputFiles = new ArrayList<String>();
	}
	
    
	/**
	 * Splits an input shapefile into a specified number of partitions.
	 * @param filePath  Path to the input shapefile
	 * @param tmpDir  Folder to hold the output partitions (i.e., files with the same extension)
	 * @param numParts  Number of partitions to be created
	 * @return  Array holding the absolute path to each of the resulting partitions
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public String[] split(String filePath, String tmpDir, int numParts, String encoding) throws IOException 
	{  
		System.out.println("Splitting " + filePath + " into " + numParts + " partitions...");
				
		File inputFile = new File(filePath);
		this.encoding = encoding;                           //Encoding of the input file, to be retained in each partition

		//Create a temporary working directory to hold the resulting partitions
		Assistant myAssistant = new Assistant();
		myAssistant.createDirectory(tmpDir);
		
		//Access the input shapefile and collect its features and schema
        Map<String, Object> map = new HashMap<>();
        map.put("url", inputFile.toURI().toURL());
        map.put("charset", encoding);

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures();      //No filter applied; all features selected.
        FeatureIterator<SimpleFeature> features = collection.features();
        SimpleFeatureType schema = collection.getSchema();               
        long numFeatures = collection.size();                                        //Size of original shapefile (in records) 
        long numPartFeatures = (long) Math.ceil((double)numFeatures / numParts);     //Size of each partition (in records)
        System.out.println("Total features: " + numFeatures + ". Number of features expected in each partition: " + numPartFeatures);
      
        int part = 0;
        long n = 0;
      
        //Create a new output file to write the first partition
        FeatureWriter<SimpleFeatureType, SimpleFeature> writer = createWriter(inputFile.getName(), tmpDir, schema, part);     
		
        //Consume input shapefile records by record and split it into the required partitions   
        try {
            while (features.hasNext()) 
            {
                SimpleFeature feature = features.next();
                writeToShapefile(feature, writer);
            	n++;
 
                //Write the collected features into the shapefile
                if ((n % numPartFeatures == 0) && (part < numParts + 1))     //Allow last partition to hold slightly extra features
                {
                	writer.close();
//                	System.out.println("Created part " + (part + 1));
                	
                	//Proceed to create a new shapefile, if there are remaining features 
                	if (n < numFeatures) 
                	{
                		part++;
                		writer = createWriter(inputFile.getName(), tmpDir, schema, part);  
                	}
                	else 
                		writer = null;
                }             
             
            }
            features.close();
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {      	
        	//Close the last partition
            if (writer != null) {
                 writer.close();
                 dataStore.dispose();
//               System.out.println("Created part " + (part + 1));
            }
        }      

        System.out.println("Partitioning concluded successfully. Input file contains " + n + " features and was split into " + (part+1) + " parts.");

		return outputFiles.toArray(new String[0]);                //Array that holds the paths to each of the resulting partitions
	}

	/**
	 * Creates a writer to be used in copying the features for a specific partition of the input shapefile
	 * @param inputFileName  The name of the shapefile containing all the input data
	 * @param tmpDir  Folder to hold the output shapefile partitions (with the same extension)
	 * @param schema  Attribute schema of input shapefile, also to be retained in the resulting partition
	 * @param part  Serial number of this partition
	 * @return  A writer to be used in copying data into the partition shapefile
	 */
	private FeatureWriter<SimpleFeatureType, SimpleFeature> createWriter(String inputFileName, String tmpDir, SimpleFeatureType schema, int part) 
	{
		File partFile = new File(tmpDir + "/" + FilenameUtils.removeExtension(inputFileName) +  "_part" + (part + 1) + fileExtension);
		outputFiles.add(partFile.getAbsolutePath());
		FeatureWriter<SimpleFeatureType, SimpleFeature> fout = null;	
        ShapefileDataStore shpDataStore;      
        ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
        HashMap<String, Serializable> params = new HashMap<String, Serializable>();
        
        //Resulting partition should have the same specifications (schema, encoding, spatial reference) as the input shapefile
		try {			
			params.put("url", partFile.toURI().toURL());
			params.put("charset", encoding);
		    shpDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
	        shpDataStore.createSchema(schema);
	        shpDataStore.forceSchemaCRS(schema.getCoordinateReferenceSystem());	               
	        fout = shpDataStore.getFeatureWriterAppend(shpDataStore.getTypeNames()[0], Transaction.AUTO_COMMIT);
	  
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fout;
	}
	
	/**
	 * Writes a given feature into a shapefile using a dedicated writer.
	 * @param f  A feature to be written into the shapefile
	 * @param writer  A writer that undertakes storing of features into a given shapefile partition
	 * @throws IOException
	 */
    private void writeToShapefile(SimpleFeature f, FeatureWriter<SimpleFeatureType, SimpleFeature> writer) throws IOException 
    {
    	SimpleFeature feature = writer.next();
    	try
    	{
	        //Copy each attribute of the original feature
	        for (AttributeDescriptor d : feature.getFeatureType().getAttributeDescriptors()) {
	        	//FIXME: Special characters in strings in several international character sets (Russian, Chinese, ...) cause trouble in .DBF format
	        	if (f.getAttribute(d.getLocalName()).toString().indexOf("�") > 0)
	        		feature.setAttribute(d.getLocalName(), f.getAttribute(d.getLocalName()).toString().substring(0, f.getAttribute(d.getLocalName()).toString().indexOf("�")-1));
	        	else
	        		feature.setAttribute(d.getLocalName(), f.getAttribute(d.getLocalName()));
	        }
	        feature.setDefaultGeometry(f.getDefaultGeometry());
	        writer.write();     
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }


	@Override
	public String[] split(String filePath, String tmpDir, int numParts) throws IOException {
		return null;
	}
    
}
