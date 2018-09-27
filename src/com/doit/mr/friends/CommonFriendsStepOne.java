package com.doit.mr.friends;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 找出共同好友
A:B,C,D,E,F
B:C,D,R
F:A,B,C
 
第一步

A-F	B
A-B	C
A-F	C
B-F	C
A-B	D


第二步
A-B	D,C,
A-F	C,B,
B-F	C,



 * @author Administrator
 * 
 */

public class CommonFriendsStepOne {

	static class StepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split(":");
			String user = split[0];
			String[] friends = split[1].split(",");
			for (String friend : friends) {
				context.write(new Text(friend), new Text(user));
			}

		}

	}

	static class StepOneReducer extends Reducer<Text, Text, Text, Text> {
		// B:F B:A
		protected void reduce(Text friend, Iterable<Text> users, Context context)
				throws IOException, InterruptedException {
			ArrayList arrayList = new ArrayList();

			for (Text user : users) {
				arrayList.add(new Text(user));
			}
			Collections.sort(arrayList);

			for (int i = 0; i < arrayList.size() - 1; i++) {
				for (int j = i + 1; j < arrayList.size(); j++) {
					context.write(
							new Text(arrayList.get(i) + "-" + arrayList.get(j)),
							new Text(friend));
				}
			}

		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
     	Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(CommonFriendsStepOne.class);

		job.setMapperClass(StepOneMapper.class);
		job.setReducerClass(StepOneReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("E:\\Hadoop\\friend\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\Hadoop\\friend\\output3"));

		job.setNumReduceTasks(1);

		boolean res = job.waitForCompletion(true);

		System.exit(res ? 0 : 1); 
	}
	
	

}
