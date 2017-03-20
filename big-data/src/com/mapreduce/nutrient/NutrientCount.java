package com.mapreduce.nutrient;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NutrientCount
{
  public static void main(String[] args)
  {
    try
    {
      Job job = Job.getInstance();
      job.setJobName("NutrientCount");
      job.setJarByClass(NutrientCount.class);
      
      job.setMapperClass(NutrientCountMapper.class);
      
      job.setReducerClass(NutrientCountReducer.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(DoubleWritable.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      boolean result = job.waitForCompletion(true);
      System.exit(result ? 0 : 1);
    }
    catch (IOException|ClassNotFoundException|InterruptedException e)
    {
      e.printStackTrace();
    }
  }
}

