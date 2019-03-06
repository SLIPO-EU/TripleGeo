/*
 * @(#) SparkPartitioner.java	version 1.7   6/3/2019
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

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.vividsolutions.jts.geom.Geometry;

import eu.slipo.athenarc.triplegeo.tools.MapToRdf;
import eu.slipo.athenarc.triplegeo.utils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.collection.mutable.WrappedArray;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Performs a transformation task using Spark, under the given configuration settings
 * @author Georgios Mandilaras
 * @version 1.7
 *
 */

/* DEVELOPMENT HISTORY
 * Created by: Georgios Mandilaras, 20/12/2018
 * Last modified: 6/3/2019
 */
public class SparkPartitioner {

    //they must be static in order to be serializable
    private static Configuration currentConfig;
    private static Classification classification;

    public  SparkPartitioner(Configuration config, Classification classific, String inFile, String outFile, int sourceSRID, int targetSRID) {
        currentConfig = config;
        classification = classific;

        String currentFormat = currentConfig.inputFormat.toUpperCase();           //Possible values: SHAPEFILE, DBMS, CSV, GPX, GEOJSON, JSON, OSM_XML, OSM_PBF, XML
        int num_partitions = currentConfig.partitions;
        Assistant myAssistant = new Assistant();


        //set spark's logger level
        Logger  rootLogger = Logger.getRootLogger();
        if (currentConfig.spark_logger_level.equals("ERROR"))
            rootLogger.setLevel(Level.ERROR);
        else if (currentConfig.spark_logger_level.equals("INFO"))
            rootLogger.setLevel(Level.INFO);
        else
            rootLogger.setLevel(Level.WARN);


        // Initialize spark's variables
        SparkConf conf = new SparkConf().setAppName("SparkTripleGeo")
                .setMaster("local[*]")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        SparkSession session = SparkSession
                .builder()
                .appName("SparkTripleGeo")
                .config(conf)
                .getOrCreate();
        SparkContext sc = session.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        try {
            //Apply data transformation according to the given input format.
            //During mapping stage, it constructs a Map that contains file's data,
            //then for each partition calls MapToRdf converter, which implements the rdf transformation.

            if (currentFormat.trim().contains("SHAPEFILE")) {

                //Checks if all the necessary files of a shapefile are included into the folder.
                if (!myAssistant.check_ShapefileFolder(inFile)) {
                    System.exit(0);
                }

                //reads dbf file's headers.
                String dbf_file = myAssistant.get_ShapeFile("dbf", inFile);
                DBFReader dbf_reader;
                List<String> dbf_fields = new ArrayList<String>();
                try {
                    dbf_reader = new DBFReader(new FileInputStream(dbf_file));
                    int numberOfFields = dbf_reader.getFieldCount();
                    for (int i = 0; i < numberOfFields; i++) {
                        DBFField field = dbf_reader.getField(i);
                        dbf_fields.add(field.getName());
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                //using GeoSpark in order to read shapefiles.
                System.setProperty("geospark.global.charset","utf8");
                SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
                spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(jsc, inFile);

                if(num_partitions > 0 )
                    spatialRDD.rawSpatialRDD = spatialRDD.rawSpatialRDD
                            .repartition(num_partitions);

                spatialRDD.rawSpatialRDD
                        .map((Function<Geometry, Map>) geometry -> {
                            String[] userData = geometry.getUserData().toString().replace(" ","").split("\t");
                            String wkt = geometry.toText();
                            Map<String, String> map = new HashMap<>();
                            map.put("wkt_geometry", wkt);
                            for (int i = 0; i < userData.length; i++){
                                map.put(dbf_fields.get(i), userData[i]);}
                            return map;
                        })
                        .foreachPartition((VoidFunction<Iterator<Map<String, String>>>) map_iter -> {
                            int partition_index = TaskContext.getPartitionId();
                            String partitions_outputFile = new StringBuilder(outFile).insert(outFile.lastIndexOf("."), "_" + partition_index).toString();

                            MapToRdf conv = new MapToRdf(currentConfig, classification, partitions_outputFile, sourceSRID, targetSRID, map_iter, partition_index);
                            conv.apply();
                            System.out.println("RDF results written into the following output files:" + partitions_outputFile.toString());
                        });

            }
            else if (currentFormat.trim().contains("CSV")) {
                //Stores csv in a dataframe
                Dataset df = session.read()
                        .format("csv")
                        .option("header", "true")
                        .option("delimiter", String.valueOf(currentConfig.delimiter))
                        .csv(inFile.split(";"));

                String[] columns = df.columns();
                JavaRDD df_rdd = df.javaRDD();
                if(num_partitions > 0 ) {
                    df_rdd = df_rdd.repartition(num_partitions);
                }

                df_rdd
                .map((Function<Row, Map>) row -> {
	                    Map<String,String> map = new HashMap<>();
	                    for(int i=0; i<columns.length; i++)
	                        try{
	                              map.put(columns[i], row.get(i).toString());
	                        }
	                        catch (NullPointerException e){}
	                    return map;
                        })
                        .foreachPartition((VoidFunction<Iterator<Map<String, String>>>) map_iter -> {
                            int partition_index = TaskContext.getPartitionId();
                            String partitions_outputFile = new StringBuilder(outFile).insert(outFile.lastIndexOf("."), "_" + partition_index).toString();

                            MapToRdf conv = new MapToRdf(currentConfig, classification, partitions_outputFile, sourceSRID, targetSRID, map_iter, partition_index);
                            conv.apply();
                            System.out.println("RDF results written into the following output files:" + partitions_outputFile.toString());
                        });
            }
            else if (currentFormat.trim().contains("GEOJSON")) {
                Dataset df = session.read()
                        .option("multiLine", true)
                        .json(inFile.split(";"));
                //Stores Geojson in a dataFrame and keeps only the necessary fields.
                Dataset featuresDF = df.withColumn("features", functions.explode(df.col("features")));
                Dataset DataDF = featuresDF.select(
                        featuresDF.col("features.type").as("type"),
                        featuresDF.col("features.properties").getItem(currentConfig.attrKey).as("key"),
                        featuresDF.col("features.properties").getItem(currentConfig.attrName).as("name"),
                        featuresDF.col("features.properties").getItem(currentConfig.attrCategory).as("category"),
                        featuresDF.col("features.geometry").getItem("type").as("geom_type"),
                        featuresDF.col("features.geometry").getItem("coordinates").as("coordinates")
                );
                DataDF.show();
                String[] columns = DataDF.columns();
                JavaRDD df_rdd = df.javaRDD();
                if(num_partitions > 0 ) {
                    df_rdd = df_rdd.repartition(num_partitions);
                }

                df_rdd
                        .map((Function<Row, Map>) row -> {
                            Map<String,String> map = new HashMap<>();
                            String geomType = null;
                            for(int i=0; i<columns.length; i++) {
                                if (columns[i].equals("geom_type"))
                                    geomType = row.get(i).toString();
                                else if(columns[i].equals("coordinates")){
                                    String coord = ((WrappedArray)row.get(i)).mkString(" ");
                                    String wkt = geomType.toUpperCase() + " (" + coord + ")";
                                    map.put("wkt_geometry", wkt);
                                }
                                else {
                                    try {
                                        map.put(columns[i], row.get(i).toString());
                                    }catch (NullPointerException e){
                                        map.put(columns[i], null);
                                    }
                                }
                            }
                            return map;
                        })
                        .foreachPartition((VoidFunction<Iterator<Map<String, String>>>) map_iter -> {
                            int partition_index = TaskContext.getPartitionId();
                            String partitions_outputFile = new StringBuilder(outFile).insert(outFile.lastIndexOf("."), "_" + partition_index).toString();

                            MapToRdf conv = new MapToRdf(currentConfig, classification, partitions_outputFile, sourceSRID, targetSRID, map_iter, partition_index);
                            conv.apply();
                            System.out.println("RDF results written into the following output files:" + partitions_outputFile.toString());
                        });
            }
            /*else if(currentFormat.trim().contains("JSON")) {
                Dataset df = session.read()
                        .option("multiLine", true)
                        .json(inFile.split(";"));
                Dataset DataDF = df.select(
                        df.col(currentConfig.attrKey).as("key"),
                        df.col(currentConfig.attrName).as("name"),
                        df.col(currentConfig.attrCategory).as("category"),
                        df.col(currentConfig.attrX).as(currentConfig.attrX),
                        df.col(currentConfig.attrY).as(currentConfig.attrY)
                );
                String[] columns = DataDF.columns();
                DataDF.show();
                JavaRDD df_rdd = df.javaRDD();
                if(num_partitions > 0 ) {
                    df_rdd = df_rdd.repartition(num_partitions);
                }
                df_rdd
                        .map((Function<Row, Map>) row -> {
                            Map<String,String> map = new HashMap<>();
                            for(int i=0; i<columns.length; i++)
                                map.put(columns[i], row.get(i).toString());
                            return map;
                        })
                        .foreachPartition((VoidFunction<Iterator<Map<String, String>>>) map_iter -> {
                            int partition_index = TaskContext.getPartitionId();
                            String partitions_outputFile = new StringBuilder(outFile).insert(outFile.lastIndexOf("."), "_" + partition_index).toString();

                            MapToRdf conv = new MapToRdf(currentConfig, classification, partitions_outputFile, sourceSRID, targetSRID, map_iter, partition_index);
                            conv.apply();
                            System.out.println("RDF results written into the following output files:" + partitions_outputFile.toString());
                        });

            }*/
            else {
                throw new IllegalArgumentException(Constants.INCORRECT_SETTING);
            }
            //System.in.read();
            session.stop();
        } catch (Exception e) {
            ExceptionHandler.abort(e, Constants.INCORRECT_SETTING);      //Execution terminated abnormally
        }

    }
}

