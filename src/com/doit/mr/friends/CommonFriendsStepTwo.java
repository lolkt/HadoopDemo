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
 * 
 * B:A，C,D C:A,D,E
 * 
 * B-A:C,D
 * 
 * @author Administrator
 * 
 */

public class CommonFriendsStepTwo {

	static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split("\t");
			String user = split[0];
			String friends = split[1];

			context.write(new Text(user), new Text(friends));

		}

	}

	static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {
		// A-B A    A-B  C
		protected void reduce(Text userPair, Iterable<Text> friends, Context context)
				throws IOException, InterruptedException {

			StringBuilder builder = new StringBuilder();

			for (Text friend : friends) {
				builder.append(friend).append(",");

			}

			context.write(userPair, new Text(builder.toString()));

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(CommonFriendsStepTwo.class);

		job.setMapperClass(StepTwoMapper.class);
		job.setReducerClass(StepTwoReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("E:\\Hadoop\\friend\\output1"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\Hadoop\\friend\\output4"));

		job.setNumReduceTasks(1);

		boolean res = job.waitForCompletion(true);

		System.exit(res ? 0 : 1);
	}

}
