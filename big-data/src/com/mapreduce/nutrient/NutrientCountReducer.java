package com.mapreduce.nutrient;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NutrientCountReducer
  extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
  protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
    throws IOException, InterruptedException
  {
    double maxNutrient = 0.0D;
    for (DoubleWritable val : values) {
      if (maxNutrient < val.get()) {
        maxNutrient = val.get();
      }
    }
    context.write(key, new DoubleWritable(maxNutrient));
  }
}
