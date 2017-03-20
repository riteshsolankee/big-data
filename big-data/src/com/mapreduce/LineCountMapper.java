package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static Text outputKey = new Text("One More Line");
	private final static IntWritable outputValue = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(outputKey, outputValue);
	}
}