package com.mml;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author meimengling
 * @version 1.0
 * @date 2020-7-23 17:50
 */
public class GroupComparator implements RawComparator<CustomWritable> {

    /**
     * 根据字节进行分组
     * int是4字节，second长度为4字节
     * first 长度 = 字节长度 - 4
     *
     * @param b1  CustomWritable序列化后的字节数组
     * @param s1  代表的是起始位置
     * @param l1  代表的是长度，b1的长度，l1-4 就是first的长度
     * @param b2  CustomWritable序列化后的字节数组
     * @param s2  代表的是起始位置
     * @param l2  代表的是长度，b2的长度
     * @return
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, 0, l1-4, b2, 0, l2-4);
    }

    /**
     * 根据类型分组
     * @param o1
     * @param o2
     * @return
     */
    @Override
    public int compare(CustomWritable o1, CustomWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}
