package com.mml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest 
{

    private FileSystem fs;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bd-server1.mml.com:8020");
        conf.set("dfs.replication", "1");
        fs = FileSystem.get(conf);
    }

    @Test
    public void testUpaload() throws IOException {
//        fs.copyFromLocalFile (new Path("e:/test.log"), new Path("/test.log.copy"));
        fs.copyFromLocalFile (new Path("E:/BaiduNetdiskDownload/hadoop/2015082818"), new Path("/webpv/input/2015082818"));
        fs.copyFromLocalFile (new Path("E:/BaiduNetdiskDownload/hadoop/2015082819"), new Path("/webpv/input/2015082819"));
        fs.close();
    }

    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false, new Path("/test.log.copy"), new Path("e:/"), true);
        fs.close();
    }
}
