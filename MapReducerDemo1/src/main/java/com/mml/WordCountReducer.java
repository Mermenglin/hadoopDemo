package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer 的输入键值对类型必须是 map 的输出键值对类型
 *
 * @author meimengling
 * @version 1.0
 * @date 2020-7-21 14:52
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable countObj = new IntWritable();

    /**
     * <p>
     * map 的结果会经过 shuffle，已经进行了分组排序，相同的key的结果已经汇总在一起
     *
     * key:value
     * hello:[1,1,2,1]
     * values 是map中结果key的汇总
     * </p>
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        countObj.set(count);
        context.write(key, countObj);
    }
}
