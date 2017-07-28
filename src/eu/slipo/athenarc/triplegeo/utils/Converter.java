/*
 * @(#) Converter.java 	 version 1.2   16/3/2017
 *
 * Copyright (C) 2013-2014 Institute for the Management of Information Systems, Athena RC, Greece.
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

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;


/**
 * Conversion Interface for TripleGeo
 * Created by: Kostas Patroumpas, 16/2/2013
 * Modified by: Kostas Patroumpas, 16/3/2017
 */
public interface Converter {  
	
     
    /**
     * 
     * Handling non-spatial attributes (CURRENTLY supporting 'name' and 'type' attributes only)
     */
    public void handleNonGeometricAttributes(String resType, String featureKey, String featureName, String featureClass) throws UnsupportedEncodingException, FileNotFoundException;

    
    /**
     * 
     * Convert representation of a geometry into suitable triples
     */
    public void parseGeom2RDF(String t, String resource, String geom, int srid);

    /**
     * 
     * Return triples from streaming RDF
     */
    public List<Triple> getTriples();
    
    /**
     * 
     * Return the model created for a dataset (graph) stored on disk
     */
    public Model getModel();
    
}
