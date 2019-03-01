/*
 * @(#) CsvPartitioner.java	version 1.7  28/2/2019
 *
 * Copyright (C) 2013-2018 Information Management Systems Institute, Athena R.C., Greece.
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;

import eu.slipo.athenarc.triplegeo.utils.Assistant;

/**
 * Splits a CSV text file into several parts in order to be concurrently transformed into RDF.
 * LIMITATIONS: Currently handling only .CSV files with a header.
 * @author Kostas Patroumpas
 * @version 1.7
 */

/* DEVELOPMENT HISTORY
 * Created by: Kostas Patroumpas, 4/10/2018
 * Last modified: 28/2/2019
 */

public class CsvPartitioner implements Partitioner {

	String fileExtension = ".csv";  //Default extension for .CSV text files
	List<String> outputFiles;       //List to hold the paths to all partitions
	
	//Constructor of this class
	public CsvPartitioner() { 
		outputFiles = new ArrayList<String>();
	}
	
	/**
	 * Splits an input file into a specified number of partitions.
	 * @param filePath  Path to the input file
	 * @param tmpDir  Folder to hold the output partitions (i.e., files with the same extension)
	 * @param numParts  Number of partitions to be created
	 * @return  Array holding the absolute path to each of the resulting partitions
	 * @throws IOException
	 */
	public String[] split(String filePath, String tmpDir, int numParts) throws IOException 
	{  	
		System.out.println("Splitting " + filePath + " into " + numParts + " partitions...");

		File inputFile = new File(filePath);
		String encoding = StandardCharsets.UTF_8.name();    //Default encoding of the input file
		
		//Create a temporary working directory to hold the resulting partitions
		Assistant myAssistant = new Assistant();
		myAssistant.createDirectory(tmpDir);
		
		long origSize = inputFile.length();                 //Size of the original file (in MB)	
		double partSize = (double) origSize / numParts;     //Size of each partition (in MB)
//		long partSize = 5 * 1024 * 1024;                    //ALTERNATIVE: Set a default max size for each partition (in MB)
		
		System.out.println("Maximum expected size of each partition: " + String.format( "%.2f", partSize /(1024 * 1024) ) + " MB");
		
		//Check for several UTF encodings and change the default one, if necessary
		BOMInputStream bomIn = new BOMInputStream(new FileInputStream(inputFile), ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE);
		if (bomIn.hasBOM(ByteOrderMark.UTF_8)) 
			encoding = StandardCharsets.UTF_8.name();
		else if (bomIn.hasBOM(ByteOrderMark.UTF_16LE)) 
			encoding = StandardCharsets.UTF_16LE.name();
		else if (bomIn.hasBOM(ByteOrderMark.UTF_16BE))
			encoding = StandardCharsets.UTF_16BE.name();
		bomIn.close();
		
		//Prepare a reader to access data from the file
		Reader fin = new InputStreamReader(new FileInputStream(inputFile), encoding);
		BufferedReader bufferedReader = new BufferedReader(fin);
		
		String line = "";
		String header = "";
		long outFileSize = 0;
		int part = 0;
		int n = 0;

        //Create a new output file to write the first partition
		BufferedWriter fout = createWriter(inputFile, tmpDir, part);
		
		//Consume input file line by line and split it into the required partitions
		while ((line = bufferedReader.readLine()) != null) 
		{
			//Keep the header of the original input file in order to reuse it in each partition
			if (n == 0) {
				header = line;		
			}
				
			//Check if size of the current partition exceeds the specified limit
		    if (outFileSize + line.getBytes().length > partSize)    //Must close this part, and...
		    { 
		    	fout.flush();
		    	fout.close();
//		        System.out.println("Created part " + (part + 1));
		        
		        //... create a new output file for each subsequent partition
		        part++;
		        fout = createWriter(inputFile, tmpDir, part);	
		        fout.write(header + "\n");        //Use the same header as the original in the new partition
		        fout.write(line + "\n");
				outFileSize = header.getBytes().length + line.getBytes().length;
		    } 
		    else    //Copy each line
		    {
		    	fout.write(line + "\n");
		        outFileSize += line.getBytes().length;
		    }
		    n++;
		} 
		//Close the last partition
		fout.flush();
		fout.close();
		bufferedReader.close();
//		System.out.println("Created part " + (part + 1));
		System.out.println("Partitioning concluded successfully. Input file contains " + n + " lines and was split into " + (part+1) + " parts.");
		
		return outputFiles.toArray(new String[0]);                //Array that holds the paths to each of the resulting partitions
	}
	
	/**
	 * Creates a buffered writer to be used in copying the contents for a specific partition of the input file
	 * @param inputFile  The file containing all the input data
	 * @param tmpDir  Folder to hold the output file partitions (with the same extension)
	 * @param part  Serial number of this partition
	 * @return  A buffered writer to be used in copying data into the partition file
	 */
	private BufferedWriter createWriter(File inputFile, String tmpDir, int part) {
		BufferedWriter fout = null;
		try {
			File partFile = new File(tmpDir + "/" + FilenameUtils.removeExtension(inputFile.getName()) +  "_part" + (part + 1) + fileExtension);
			outputFiles.add(partFile.getAbsolutePath());
			fout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(partFile, false), StandardCharsets.UTF_8));
		} catch (FileNotFoundException ex) {
			    ex.printStackTrace();
		}	
		return fout;
	}

	@Override
	public String[] split(String filePath, String tmpDir, int numParts, String encoding) throws IOException {
		return null;
	}
	
}
