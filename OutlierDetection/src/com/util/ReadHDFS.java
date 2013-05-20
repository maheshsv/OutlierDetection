package com.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
	public static String readHDFS(String inputFileStr) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(
						fs.open(new Path(inputFileStr))));
		String line = null;
		String outputStr = "";
		while((line = reader.readLine()) != null){
			String[] buf = line.split("\t");
			outputStr += buf[0] + ":" + buf[1] +";";
		}
		
		// remove the last ;
		outputStr = outputStr.substring(0, outputStr.length() - 1);
		return outputStr;
	}
}
