package com.mapreduce.nutrient;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NutrientCountMapper
  extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
  public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
    throws IOException, InterruptedException
  {
    String line = value.toString();
    
    String[] data = line.split(",");
    try
    {
      Text outputKey = new Text(data[0].toUpperCase().trim());
      DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(data[5]));
      context.write(outputKey, outputValue);
    }
    catch (NumberFormatException npe)
    {
      npe.printStackTrace();
    }
  }
}

