/*
 * @(#) Partitioner.java 	 version 1.8   28/2/2019
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

import java.io.IOException;

/**
 * Partitioning Interface for TripleGeo used in splitting input files into several equi-sized pieces for applying concurrent transformation to RDF.
 * @author Kostas Patroumpas
 * @version 1.8
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 4/10/2018
 * Last modified: 28/2/2019
 */
public interface Partitioner {

	/**
	 * Splits an input .CSV file into a specified number of partitions.
	 * @param filePath  Path to the input .CSV file
	 * @param tmpDir  Folder to hold the output partitions (i.e., files with the same extension)
	 * @param numParts  Number of partitions to be created
	 * @return  Array holding the absolute path to each of the resulting partitions
	 * @throws IOException
	 */
	public String[] split(String filePath, String tmpDir, int numParts) throws IOException;
	
	/**
	 * Splits an input shapefile into a specified number of partitions.
	 * @param filePath  Path to the input shapefile
	 * @param tmpDir  Folder to hold the output partitions (i.e., files with the same extension)
	 * @param numParts  Number of partitions to be created
	 * @return  Array holding the absolute path to each of the resulting partitions
	 * @throws IOException
	 */
	public String[] split(String filePath, String tmpDir, int numParts, String encoding) throws IOException;
	
}
