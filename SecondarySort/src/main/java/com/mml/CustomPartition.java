package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 这里面的key，value 是map输出的key，value类型
 *
 * @author meimengling
 * @version 1.0
 * @date 2020-7-23 17:42
 */
public class CustomPartition extends Partitioner<CustomWritable, IntWritable> {

    /**
     *
     * @param customWritable
     * @param intWritable
     * @param numPartitions reduce task的个数
     * @return
     */
    @Override
    public int getPartition(CustomWritable customWritable, IntWritable intWritable, int numPartitions) {
        return (customWritable.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
