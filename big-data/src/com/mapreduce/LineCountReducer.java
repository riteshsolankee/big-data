package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class LineCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private final static Text outputKey = new Text("Total Lines");

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int numLines = 0;
		for (IntWritable val : values) {
			numLines += val.get();
		}

		context.write(outputKey, new IntWritable(numLines));
	}
}
