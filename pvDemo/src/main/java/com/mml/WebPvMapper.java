package com.mml;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebPvMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private IntWritable outputValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将每一行转成String类型，方便操作
        String line = value.toString();

        // 根据制表符切割字符串，并保存到String数组里
        String[] fields = line.split("\t");

        if (fields.length < 30) {
            context.getCounter("WEBPV_COUNTER INFO", "当前行长度太短").increment(1L);
            return;
        }

        if (StringUtils.isBlank(fields[1])) {
            // 计数，并在日志输出
            context.getCounter("WEBPV_COUNTER INFO", "url不存在").increment(1L);
            return;
        }

        String province = fields[23];

        if (StringUtils.isBlank(province)) {
            context.getCounter("WEBPV_COUNTER INFO", "省ID为空").increment(1L);
            return;
        }

        if (NumberUtils.isNumber(province)) {
            outputKey.set(Integer.valueOf(province));
        } else {
            context.getCounter("WEBPV_COUNTER INFO", "省ID不是数字").increment(1L);
            return;
        }

        outputValue.set(1);
        context.write(outputKey, outputValue);


    }

}
