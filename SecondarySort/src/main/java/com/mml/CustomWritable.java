package com.mml;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author meimengling
 * @version 1.0
 * @date 2020-7-23 16:35
 */
public class CustomWritable implements WritableComparable<CustomWritable> {

    // 自定义两个属性
    private String first;
    private int second;

    @Override
    public int compareTo(CustomWritable o) {
        int result = this.first.compareTo(o.getFirst());
        if (result == 0) {
            return Integer.valueOf(second).compareTo(o.getSecond());
        }
        return result;
    }

    /**
     * 对数据进行序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    /**
     * 对数据进行反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.setFirst(in.readUTF());
        this.setSecond(in.readInt());
    }

    public CustomWritable() {
    }

    public CustomWritable(String first, int second) {
        set(first, second);
    }

    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
