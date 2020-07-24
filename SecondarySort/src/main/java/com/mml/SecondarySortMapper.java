package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author meimengling
 * @version 1.0
 * @date 2020-7-23 16:34
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CustomWritable, IntWritable> {

    private CustomWritable outputKey = new CustomWritable();
    private IntWritable outputValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = line.split("\t");

        String first = words[0];
        Integer second = Integer.valueOf(words[1]);

        outputKey.set(first, second);
        outputValue.set(second);

        context.write(outputKey, outputValue);
    }
}
