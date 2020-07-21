package com.mml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author meimengling
 * @version 1.0
 * @date 2020-7-21 15:10
 */
public class WordCountDriver {

    /**
     *
     * @param args 指定数据的输入路径和结果的输出路径
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置数据的输入路径以及结果的输出路径
//        args = new String[]{
//                "hdfs://bd-server1.mml.com:8020/wordcount/input",
//                "hdfs://bd-server1.mml.com:8020/wordcount/output1"
//        };

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bd-server1.mml.com:8020");
        conf.set("dfs.replication", "1");

        // 要拿到job对象，用于提交mapreduce任务到yarn集群
        Job job = Job.getInstance(conf);

        // 设定程序的主类，否则yarn找不到这个mapreduce程序
        job.setJarByClass(WordCountDriver.class);

        // 指定数据的输入路径以及结果的输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 指定我们自定义的map程序和reduce程序
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置map的输出键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reduce的输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
