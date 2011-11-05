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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 * 
 * @editor Stephen, TAK-LON WU (taklwu@indiana.edu)
 */

public class RunnerMap extends Mapper<String, String, IntWritable, Text> {
	
	private String localDB = "";
	private String localBlastProgram = "";
	
    private FileSystem fs;
	
	@Override
	public void setup(Context context) throws IOException{
		System.out.println("from here");
		if (context == null) {
			System.out.println("context is null");
		}
		Configuration conf = context.getConfiguration();
		if (conf == null) {
			System.out.println("conf is null");
		}

		
		Path[] local = DistributedCache.getLocalCacheArchives(conf);
		if (local[0] == null) {
			System.out.println("local is null");
		}
		this.localDB = local[0].toUri().getPath() + File.separator + conf.get(HadoopBlast.DB_ARCHIVE_DIR) + File.separator + conf.get(HadoopBlast.DB_NAME);
		System.out.println(localDB);
		this.localBlastProgram = local[0].toUri().getPath();
		System.out.println(localBlastProgram);
		/*
		 * Your code here
		 */
	}
	

	public void map(String key, String value, Context context) throws IOException,
		InterruptedException {
		
		Long startTime = System.currentTimeMillis();
		long endTime;
		
		Configuration conf = context.getConfiguration();
		String binAndDbArchive = conf.get(HadoopBlast.Bin_DB_Archive);
		String execName = conf.get(HadoopBlast.EXECUTABLE);
		String cmdArgs = conf.get(HadoopBlast.PARAMETERS);
		String outputDir = conf.get(HadoopBlast.OUTPUT_DIR);
		String workingDir = conf.get(HadoopBlast.WORKING_DIR);
		
		/*
		 * Your code here
		 */
		String[] tmp = value.split(File.separator);
		String fileNameOnly = tmp[tmp.length - 1];
		
		String localInputPathStr = workingDir + File.separator + fileNameOnly;
		String localOutputPathStr = workingDir + File.separator + fileNameOnly + ".out";
		
		
		Path hdfsSplitInputPath = new Path(value);
		fs = hdfsSplitInputPath.getFileSystem(conf);
		
		fs.copyToLocalFile(hdfsSplitInputPath, new Path(localInputPathStr));
		
		String execCommand = cmdArgs.replaceAll("#_INPUTFILE_#", localInputPathStr);
		if (cmdArgs.indexOf("#_OUTPUTFILE_#") > -1) {
			execCommand = execCommand.replaceAll("#_OUTPUTFILE_#", localOutputPathStr);
		} else {
			localOutputPathStr = localInputPathStr;
		}
		
		execCommand = localBlastProgram + File.separator + execName + " " + execCommand + " -db " + this.localDB;
		System.out.println(execCommand);
		
		Process proc = Runtime.getRuntime().exec(execCommand);
		
		OutputHandler errInfo = new OutputHandler(proc.getErrorStream(), "err", workingDir + File.separator + fileNameOnly + ".err");
		errInfo.start();
		OutputHandler outInfo = new OutputHandler(proc.getInputStream(), "out", fileNameOnly + ".out");
		outInfo.start();

		try {
			if (proc.waitFor() != 0) {
				System.err.println("exit value = " + proc.exitValue());
			}
		} catch (InterruptedException ie) {
			System.err.println(ie);
			ie.printStackTrace();			
		}
		
		Path hdfsOutputDirPath = new Path(outputDir);
		Path hdfsOutputFilePath = new Path(hdfsOutputDirPath, fileNameOnly + ".out");
		
		fs.copyFromLocalFile(new Path(localOutputPathStr), hdfsOutputFilePath);
		
		endTime = System.currentTimeMillis() - startTime;
		
		double runningTime = endTime * 1.0 / 1000;
		
		System.out.println("time for processing " + fileNameOnly + ": " + runningTime + " seconds.");
		
	}
}
