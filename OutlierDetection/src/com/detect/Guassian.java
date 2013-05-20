package com.detect;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.ReadHDFS;

public class Guassian {
	/**
	 * compute mean
	 */
	public static class ComputeMeanMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		private String separator = null;
		
		public void setup(Context context) {
			separator = context.getConfiguration().get("separator");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context){
			String[] buf = value.toString().trim().split(separator);
			
			for(int i = 0; i < buf.length; i++){
				try {
					if (!buf[i].equals(" ") && buf[i] != null){
						context.write(new Text(String.valueOf(i)), new Text(buf[i]));
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class ComputeMeanReducer extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context){
			Iterator<Text> iter = values.iterator();
			int num = 0;
			Double sum = 0d;
			while(iter.hasNext()){
				num++;
				sum += Double.valueOf(iter.next().toString());
			}
			
			Double mean = sum / num;
			try {
				context.write(key, new Text(String.valueOf(mean)));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * compute Var
	 */
	public static class ComputeVarMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		private String separator = null;
		
		public void setup(Context context) {
			separator = context.getConfiguration().get("separator");
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context){
			String[] buf = value.toString().trim().split(separator);
			
			for(int i = 0; i < buf.length; i++){
				try {
					if (!buf[i].equals(" ") && buf[i] != null){
						context.write(new Text(String.valueOf(i)), new Text(buf[i]));
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class ComputeVarReducer extends
	Reducer<Text, Text, Text, Text> {
		private String meanStr = null; 
		private String meanInfo = null;
		
		/**
		 * read mean from mean path
		 */
		public void setup(Context context) {
			meanStr = context.getConfiguration().get("meanStr");
			//
			meanStr += "//part-r-00000";
			try {
				meanInfo = ReadHDFS.readHDFS(meanStr);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context){
			Iterator<Text> iter = values.iterator();
			int num = 0;
			Double sum = 0d;
			String[] buf = meanInfo.split(";");
			Double[] mean = new Double[buf.length];
			for (int i = 0; i < buf.length; i++){
				String[] temp = buf[i].split(":");
				mean[Integer.parseInt(temp[0])] = Double.valueOf(temp[1]);
			}
			
			int index = Integer.parseInt(key.toString());
			while(iter.hasNext()){
				num++;
				Double delta = Double.valueOf(iter.next().toString()) - mean[index];
				sum += delta * delta;
			}
			
			Double var = sum / num;
			try {
				context.write(key, new Text(String.valueOf(var)));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * compute P
	 */
	public static class ComputePMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		private String separator = null;
		private String meanStr = null; 
		private String meanInfo = null;
		private String varStr = null; 
		private String varInfo = null;
		
		public void setup(Context context) {
			separator = context.getConfiguration().get("separator");
			meanStr = context.getConfiguration().get("meanStr");
			//
			meanStr += "//part-r-00000";
			
			varStr = context.getConfiguration().get("varStr");
			//
			varStr += "//part-r-00000";
			try {
				meanInfo = ReadHDFS.readHDFS(meanStr);
				varInfo = ReadHDFS.readHDFS(varStr);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		/**
		 * For wisconsin data, buf[0] is id, buf[len-1] is label.
		 * var is variance, square of standard deviation. 
		 */
		@Override
		public void map(LongWritable key, Text value, Context context){
			String[] buf = value.toString().trim().split(separator);
			String outKeyStr = buf[buf.length-1] + ":" + buf[0];
			
			String[] meanbuf = meanInfo.split(";");
			Double[] mean = new Double[meanbuf.length];
			for (int i = 0; i < meanbuf.length; i++){
				String[] temp = meanbuf[i].split(":");
				mean[Integer.parseInt(temp[0])] = Double.valueOf(temp[1]);
			}
			
			String[] varbuf = varInfo.split(";");
			Double[] var = new Double[varbuf.length];
			for (int i = 0; i < varbuf.length; i++){
				String[] temp = varbuf[i].split(":");
				var[Integer.parseInt(temp[0])] = Double.valueOf(temp[1]);
			}
			
			Double p = 1d;
			for(int i = 1; i < buf.length - 1; i++){
				if (!buf[i].equals(" ") && buf[i] != null){
					p *= 1.0 / (Math.sqrt(2*Math.PI*var[i-1])) 
							* Math.exp(-1.0 * 
									    Math.pow(Double.parseDouble(buf[i]) - mean[i-1], 2)
									    / (2 * var[i-1])
									    );
				}	
			}
			
			try {
				context.write(new Text(outKeyStr), new Text(String.valueOf(p)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		String trainStr = "/user/hadoop/outliers/input/wisconsin_train.txt";
		String testStr = "/user/hadoop/outliers/input/wisconsin_test.txt";
		String meanStr = "/user/hadoop/outliers/output/mean";
		String varStr = "/user/hadoop/outliers/output/var";
		String pStr = "/user/hadoop/outliers/output/probability";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path meanPath = new Path(meanStr);
		if (fs.exists(meanPath))
			fs.delete(meanPath, true);
		
		Path varPath = new Path(varStr);
		if (fs.exists(varPath))
			fs.delete(varPath, true);
		
		Path pPath = new Path(pStr);
		if (fs.exists(pPath))
			fs.delete(pPath, true);
		
		
		String separator = ",";  
		conf.set("separator", separator);

		Job job = new Job(conf, "Compute mean...");
		job.setMapperClass(ComputeMeanMapper.class);
		job.setReducerClass(ComputeMeanReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(trainStr));
		FileOutputFormat.setOutputPath(job, meanPath);
		job.setJarByClass(Guassian.class);
		job.waitForCompletion(true);

		System.out.println("Compute mean done.");
		
		conf.set("separator", separator);
		conf.set("meanStr", meanStr);
		
		Job jobVar = new Job(conf, "Compute Var...");
		jobVar.setMapperClass(ComputeVarMapper.class);
		jobVar.setReducerClass(ComputeVarReducer.class);
		jobVar.setMapOutputKeyClass(Text.class);
		jobVar.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(jobVar, new Path(trainStr));
		FileOutputFormat.setOutputPath(jobVar, varPath);
		jobVar.setJarByClass(Guassian.class);
		jobVar.waitForCompletion(true);

		System.out.println("Compute var done.");
		
		conf.set("separator", separator);
		conf.set("meanStr", meanStr);
		conf.set("varStr", varStr);
		
		Job jobP = new Job(conf, "Compute probabiltiy...");
		jobP.setMapperClass(ComputePMapper.class);
		jobP.setMapOutputKeyClass(Text.class);
		jobP.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(jobP, new Path(testStr));
		FileOutputFormat.setOutputPath(jobP, pPath);
		jobP.setJarByClass(Guassian.class);
		jobP.waitForCompletion(true);

		System.out.println("Compute probabiltiy done.");
	}
}
