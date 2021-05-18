package com.zhu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/4 22:45
 */
public class HdfsClientsTest {

    /**
     * URI
     */
    private static final String URI = "hdfs://hadoop2:8020";

    /**
     * User
     */
    private static final String USER = "zhu";

    private FileSystem fileSystem = null;

    @Before
    public void base() throws Exception {
        URI uri = new URI(URI);
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(uri, configuration, USER);
    }

    @After
    public void after() throws Exception {
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws Exception {
        fileSystem.mkdirs(new Path("/mkdir"));
    }

    @Test
    public void upload() throws Exception {
        fileSystem.copyFromLocalFile(new Path("D:\\demo.txt"), new Path("/upload"));
    }

    @Test
    public void download() throws Exception {
        fileSystem.copyToLocalFile(new Path("/upload"),new Path("C:\\Users\\18380\\Desktop"));
    }

}
