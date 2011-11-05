/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or NaradaBrokering, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called NaradaBrokering, 
 * nor may Indiana University or Community Grids Lab or NaradaBrokering appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */
package edu.indiana.cs.b649;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.cs.DataFileInputFormat;
import java.net.URI;
/**
 * Blast+/Cap3 Data analysis using Hadoop MapReduce.
 * This program demonstrated a usage of "map-only" operation to execute
 * a data analysis application on a collection of data files.
 * 
 * Blast+/Cap3 is a gene sequencing program which consumes a *.fa/*.fsa file and 
 * produces several output files along with the standard out.
 * 
 * The data is placed in a shared file system (or can be placed on all the local disks)
 * and the file names are written to HDFS. For hadoop, the data file names becomes the 
 * data.
 * 
 * Hadoop executes each map task by passing a data file name as the value parameter.
 * Map task execute Blast+/Cap3 program (written in C++/C) and save the standard output into a 
 * file. It can also be used to copy these output files to a predefined location.
 * 
 * @author Jaliya Ekanayake (jekanaya@cs.indiana.edu)
 * 03/03/2009
 * 
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 * 2009-2010
 *
 *
 * @editor Stephen, TAK-LON WU (taklwu@indiana.edu)
 * 2010-2011
 */
public class HadoopBlast extends Configured implements Tool {

	public static String Bin_DB_Archive = "BlastProgramAndDB.tar.gz";
	public static String EXECUTABLE = "execName";
	public static String WORKING_DIR = "workingDir";
	public static String DB_ARCHIVE_DIR = "db";
	public static String DB_NAME = "nr";
	public static String OUTPUT_DIR = "outDir";
	public static String PARAMETERS = "cmd";

/**
 * Launch the MapReduce computation.
 * This method first, remove any previous working directories and create a new one
 * Then the data (file names) is copied to this new directory and launch the 
 * MapReduce (map-only though) computation.
 * @param numReduceTasks - Number of reduce tasks = 0.
 * @param binAndDbArchive - the uploaded databaseArchive filename on HDFS
 * @param execName - Name of the binary executable.
 * @param workingDir - the local disk working directory when computing the downloaded *.fa from HDFS
 * @param databaseArchiveDir - The directory where the Blast+/Cap3 program is after unzip the distributed cached archive. 
 * @param databaseName - the Blast+ database name, normally "nr"   
 * @param inputDir - Directory where the input data set is located on HDFS.
 * @param outputDir - Output directory to place the output on HDFS.
 * @param cmdArgs - These are the command line arguments to the Blast+ program.
 * @throws Exception - Throws any exception occurs in this program.
 * 
 * you are free to change this launch function to support your own program
 */
	void launch(int numReduceTasks, String binAndDbArchive,
			String execName, String workingDir, String databaseArchiveDir, String databaseName, String inputDir, String outputDir, String cmdArgs) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, execName);

		Path hdMainDir = new Path(outputDir);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(hdMainDir, true);
		Path hdOutDir = new Path(hdMainDir, "out");

		Configuration jc = job.getConfiguration();

		jc.set(Bin_DB_Archive, binAndDbArchive); // this the name of the executable archive
		jc.set(EXECUTABLE, execName);
		jc.set(WORKING_DIR, workingDir);
		jc.set(DB_ARCHIVE_DIR, databaseArchiveDir);
		jc.set(DB_NAME, databaseName);
		jc.set(OUTPUT_DIR, outputDir);
		jc.set(PARAMETERS, cmdArgs);
		jc.set(OUTPUT_DIR, outputDir);
		
		FileInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, hdOutDir);		
		
		DistributedCache.addCacheArchive(new URI(Bin_DB_Archive), jc);


		/*
		 * Your code here
		 */
		System.out.println("so far so good");
		job.setJarByClass(HadoopBlast.class);
        job.setMapperClass(RunnerMap.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DataFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(numReduceTasks);

		

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		//clean the cache

		
		System.exit(exitStatus);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 8) {
			System.err.println("Usage: HadoopBlast <Executable and Database Archive on HDFS> <Executable> <Working_Dir> <Database dir under archive> <Database name> <HDFS_Input_dir> <HDFS_Output_dir> <Cmd args>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		int numReduceTasks = 0;// We don't need reduce here.
		String binAndDbArchive = args[0];
		String execName = args[1];
		String workingDir = args[2];
		String databaseDir = args[3];
		String databaseName = args[4];
		String inputDir = args[5];
		String outputDir = args[6];
		//"-query #_INPUTFILE_# -outfmt 6 -seg no -out #_OUTPUTFILE_#"
		String cmdArgs = args[7] ;
		
		launch(numReduceTasks, binAndDbArchive, execName, workingDir , databaseDir, databaseName, inputDir,
				outputDir, cmdArgs);
		return 0;
	}

	public static void main(String[] argv) throws Exception {
		long startTime = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new HadoopBlast(), argv);
		
		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		System.exit(res);
	}
}
