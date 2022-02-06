package com.jayurbain.emr.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieRecommendation {

   public static class Map extends Mapper<LongWritable, Text, IntWritable, Text > {
      private final static IntWritable outKey = new IntWritable(1);
      private Text itemID2rating = new Text();

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] tokens = line.split("::");
         int userId = Integer.parseInt(tokens[0]);
         String itemID = tokens[1];
         String rating = tokens[2];
         outKey.set(userId);
         itemID2rating.set(itemID + "::" + rating);
         context.write(outKey, itemID2rating);
      }
   }

   public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

      private Text ratingValues = new Text();
      public void reduce(IntWritable key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException {
         StringBuilder sb = new StringBuilder();
         int item_count = 0;
         int item_sum = 0;
         for (Text val : values) {
            String itemId_rating = val.toString();
            String[] tokens = itemId_rating.split("::");
            int rating = Integer.parseInt(tokens[1]);
            int item_id = Integer.parseInt(tokens[0]);
            item_count += 1;
            item_sum += rating;
            sb.append("," + item_id  + ";" + rating) ;
         }
         ratingValues.set(item_count + ":" + item_sum + ":" + sb.toString().replaceFirst(",",""));
         context.write(key, ratingValues);
      }
   }

   public static class Step2Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text outKey = new Text();
      private Text outValue = new Text();

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] tokens = line.split(":");
         String values = tokens[2];
         String[] valuePair = values.split(",");
         for(int i = 1; i < valuePair.length; i++) {
            String itemID = valuePair[i].split(";")[0];
            String rating1 = valuePair[i].split(";")[1];
            for(int j = 1; j < valuePair.length; j++) {
               String itemID2 = valuePair[j].split(";")[0];
               String rating2 = valuePair[j].split(";")[1];
               outKey.set(itemID + "::" + itemID2);
               outValue.set(rating1 + "::" + rating2);
               context.write(outKey, outValue);
            }
         }
      }
   }

   public static class ReduceStep2 extends Reducer<Text, Text, Text, Text> {

      private final static FloatWritable result = new FloatWritable(1.0F);
      private Text result2 = new Text();
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

         StringBuilder sb = new StringBuilder();
         int sum_X = 0, sum_Y = 0, sum_XY = 0;
         int squareSum_X = 0, squareSum_Y = 0;
         int n = 0;

         for (Text val : values) {
            int rating_1 = Integer.parseInt(val.toString().split("::")[0]);
            int rating_2 = Integer.parseInt(val.toString().split("::")[1]);
            // sum of elements of array X.
            sum_X = sum_X + rating_1;

            // sum of elements of array Y.
            sum_Y = sum_Y + rating_2;

            // sum of X[i] * Y[i].
            sum_XY = sum_XY + rating_1 * rating_2;

            // sum of square of array elements.
            squareSum_X = squareSum_X + rating_1 * rating_1;
            squareSum_Y = squareSum_Y + rating_2 * rating_2;

            n += 1;
         }
         float corr = (float)(n * sum_XY - sum_X * sum_Y)/
                 (float)(Math.sqrt((n * squareSum_X -
                         sum_X * sum_X) * (n * squareSum_Y -
                         sum_Y * sum_Y)));

         if(!Float.isNaN(corr)) {
            result2.set(String.valueOf(corr));
            context.write(key, result2);
         }
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();

      Job job = new Job(conf, "Step_1");

      job.setJarByClass(MovieRecommendation.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      boolean isSuccess = job.waitForCompletion(true);

      if(isSuccess) {

         Job job2 = new Job(conf, "Step_2");
         job2.setJarByClass(MovieRecommendation.class);
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(Text.class);

         job2.setMapperClass(Step2Map.class);
         job2.setReducerClass(ReduceStep2.class);

         job2.setInputFormatClass(TextInputFormat.class);
         job2.setOutputFormatClass(TextOutputFormat.class);

         FileInputFormat.addInputPath(job2, new Path(args[1]));
         FileOutputFormat.setOutputPath(job2, new Path(args[2]));

         job2.waitForCompletion(true);

      }


   }

}