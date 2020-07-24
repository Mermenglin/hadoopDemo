package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author meimengling
 * @version 1.0
 * @date 2020-7-23 17:58
 */
public class SecondarySortReducer extends Reducer<CustomWritable, IntWritable, Text, IntWritable> {

    private Text outputKey = new Text();


    @Override
    protected void reduce(CustomWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        outputKey.set(key.getFirst());

        for (IntWritable value : values) {
            context.write(outputKey, value);
        }
    }
}
