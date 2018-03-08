/*
 * @(#) Converter.java 	 version 1.4   27/2/2018
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVRecord;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.geotools.feature.FeatureIterator;
import org.opengis.referencing.operation.MathTransform;

import be.ugent.mmlab.rml.model.dataset.RMLDataset;
import eu.slipo.athenarc.triplegeo.osm.OSMRecord;

/**
 * Conversion Interface for TripleGeo used in transformation of spatial features (including their non-spatial attributes) into RDF triples with various serializations.
 * @author Kostas Patroumpas
 * @version 1.4
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 16/2/2013
 * Last modified: 27/2/2018
 */
public interface Converter {  
    
	/**
	 * Parses each record from a FeatureIterator and creates the resulting triples (including geometric and non-spatial attributes).
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param iterator  FeatureIterator over spatial features collected from an ESRI shapefile of a GeoJSON file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, FeatureIterator<?> iterator, Classification classific, MathTransform reproject, int targetSRID, String outputFile);

	
	/**
	 * Parses each record from a ResultSet and creates the resulting triples (including geometric and non-spatial attributes).
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  ResultSet containing spatial features retrieved from a DBMS.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, ResultSet rs, Classification classific, MathTransform reproject, int targetSRID, String outputFile);


	/**
	 * Parses each record from a collection of CSV records and creates the resulting triples (including geometric and non-spatial attributes).
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param records  Iterator over CSV records collected from a CSV file.
	 * @param classific  Instantiation of the classification scheme that assigns categories to input features.
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void parse(Assistant myAssistant, Iterator<CSVRecord> records, Classification classific, MathTransform reproject, int targetSRID, String outputFile);
    
	
	/**
	 * Parses a single OSM record and creates the resulting triples (including geometric and non-spatial attributes)
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param rs  Representation of an OSM record with attributes extracted from an OSM element (node, way, or relation).
	 * @param reproject  CRS transformation parameters to be used in reprojecting a geometry to a target SRID (EPSG code).
	 * @param targetSRID  Spatial reference system (EPSG code) of geometries in the output RDF triples.
	 */
	public void parse(Assistant myAssistant, OSMRecord rs, MathTransform reproject, int targetSRID);
	

	/**
	 * Stores resulting tuples into a file.	
	 * @param myAssistant  Instantiation of Assistant class to perform auxiliary operations (geometry transformations, auto-generation of UUIDs, etc.)
	 * @param outputFile  Path to the output file that collects RDF triples.
	 */
	public void store(Assistant myAssistant, String outputFile);
	

	/**
	 * Retains the header (column names) of an input CSV file.
	 * @param header  A array of attribute (column) names.
	 */
	public void setHeader(String[] header);
	

	/**
	 * Converts a row (with several attribute values, including geometry) into suitable triples according to the specified RML mappings. Applicable in RML transformation mode.
	 * @param row  Record with attribute names and their respective values.
	 * @param dataset RMLDataset to collect the resulting triples.
	 */
	public void parseWithRML(HashMap<String, String> row, RMLDataset dataset);
	

	/**
	 * Provides triples resulted after applying transformation in STREAM mode.
	 * @return A collection of RDF triples.
	 */
    public List<Triple> getTriples();
    
 
    /**
     * Provides access to the disk-based model consisting of transformed triples. Applicable in GRAPH transformation mode.
     * @return The model created for a dataset (RDF graph) stored on disk.
     */
    public Model getModel();

    /**
	 * Returns the local directory that holds the disk-based RDF graph for this transformation thread. Applicable in GRAPH transformation mode.
	 * @return  Path to the local directory. 
	 */
    public String getTDBDir();

    /**
     * Serializes the given RML dataset as triples written into a file. 
     * Applicable in RML transformation mode only.
     * @param dataset RMLDataset that has collected the resulting triples.
     * @param writer  BufferedWriter to write triples in the output stream.
     * @param rdfFormat  Serialization format of triples to be written to the file.
     * @param encoding  Encoding for string literals.
     * @return  The number of triples written to the output file.
     * @throws IOException
     */
	public int writeTriples(RMLDataset dataset, BufferedWriter writer, org.openrdf.rio.RDFFormat rdfFormat, String encoding) throws IOException;
	
	
	/**
     * Serializes the given RML dataset as triples written into a file.
     * Applicable in RML transformation mode only.
     * @param dataset RMLDataset that has collected the resulting triples.
     * @param writer  BufferedWriter to write triples in the output stream.
     * @param rdfFormat  Serialization format of triples to be written to the file.
	 * @return   The number of triples written to the output file.
	 * @throws IOException
	 */
	public int writeTriples(RMLDataset dataset, OutputStream writer, org.openrdf.rio.RDFFormat rdfFormat) throws IOException;

	
	/**
	 * Provides the URI template used for all subjects in RDF triples concerning the classification hierarchy.
	 * Applicable in RML transformation mode.
	 * @return A URI to be used as template for triples regarding classification.
	 */
	public String getURItemplate4Classification();

	
}
