package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer 的输入键值对类型必须是 map 的输出键值对类型
 */
public class WebPvReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable countObj = new IntWritable();


    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        countObj.set(count);
        context.write(key, countObj);
    }
}
