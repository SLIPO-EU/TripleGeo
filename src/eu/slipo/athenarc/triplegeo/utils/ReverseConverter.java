/*
 * @(#) ReverseConverter.java 	 version 1.7   24/2/2018
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

import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;

/**
 * Reverse Conversion Interface for TripleGeo used in reconstructing (relational) records from an RDF graph.
 * @author Kostas Patroumpas
 * @version 1.7
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 8/12/2017
 * Modified: 11/12/2017; support for auto-generation of attribute schema
 * Last modified: 24/2/2018
 */
public interface ReverseConverter {  

	/**
	 * Closes output file, once all results have been written.
	 */
	public void close();


	/**
	 * Determines schema with all attribute names and their respective data types.
	 * @param attrList  List of attribute names.
	 * @param sol  A single result (record) from a SELECT query in SPARQL.
	 * @return  True if attribute schema has been created successfully; otherwise, False.
	 */
	public boolean createSchema(List<String> attrList, QuerySolution sol);
	 

	/**
	 * Identifies SRID (EPSG code) and geometry type from a WKT literal. Applicable in reverse transformation into shapefile format.
	 * @param g  A geometry WKT literal.
	 * @return  A string that concatenates the geometry type and the SRID of the given WKT literal.
	 */
	public String getGeometrySpec(Literal g);
	
	/**
	 * Identifies SRID (EPSG code) and geometry type from a WKT string. Applicable in reverse transformation into CSV format.
	 * @param g  A geometry WKT string.
	 * @return  An extended WKT (EWKT) representation, which includes the SRID.
	 */
	public String getGeometrySpec(String g);
	
	/**
	 * Creates a feature collection from current batch of records and writes them into the output file.
	 * @param attrList  List of attribute names.
	 * @param results  Collection of records in the current batch.
	 * @return  The number of records resulted in the current batch.
	 */
	public Integer store(List<String> attrList, List<List<String>> results);
	
}
