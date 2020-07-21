package com.mml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 输入键值对，输出键值对
 * KEYIN  是每行首字符相当于文件首字符的偏移量，通常是long类型，但在hadoop中需要跨节点传输，所以hadoop提供了一套自己的序列化的数据类型，
 *        long的类型是 LongWritable
 *
 * VALUEIN 是每行的字符，是String类型，对应hadoop中就是 Text
 *
 * KEYOUT  是map结果输出的键值对key类型，是一个单词，应该是String类型，对应 hadoop中是 Text
 *
 * VALUEOUT  是map结果输出的键值对value，是单数数，应该是int类型，对应 hadoop中是 IntWritable
 *
 * </p>
 *
 * @author meimengling
 * @version 1.0
 * @date 2020-7-21 14:12
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text wordObj = new Text();
    private IntWritable countObj = new IntWritable();

    /**
     * <p>
     *     重写 map方法
     * </p>
     *
     * @param key 是map读数据的键值对的key值，是字节偏移量
     * @param value 是map读数据的每行字符
     * @param context 是上下文的对象，用于结果的键值对输出
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将每一行转成String类型，方便操作
        String line = value.toString();
        // 将字符串按空格切割
        String[] words = line.split(" ");

        // 直接输出，汇总计算放到reducer中做
        for (String word : words) {
            wordObj.set(word);
            countObj.set(1);
            context.write(wordObj, countObj);
        }

/*
        Map<String, Integer> map = new HashMap<String, Integer>(32);
        for (String word : words) {
            if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 1);
            }
        }
        for (String word : map.keySet()) {
            wordObj.set(word);
            countObj.set(map.get(word));
            context.write(wordObj, countObj);
        }*/

    }

}
