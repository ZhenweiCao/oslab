package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

public class HDFSTest {
    public HDFSTest(){}
    public static void main(String[] args){
        Test hdfs = new Test();
        try{
            hdfs.run();
            System.out.println("hello");
        } catch (Exception e){
            System.out.println(e);
        } finally {
            System.out.println("this is finally");
        }
    }
}

class Test{
    public Test(){}
    public void run() throws Exception {
        String uri = "hdfs://127.0.0.1:9000/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        FileStatus[] statuses = fs.listStatus(new Path("/user/oslab"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        FSDataOutputStream os = fs.create(new Path("/user/oslab/input/test.log"));
        os.write("Hello World!".getBytes());
        os.flush();
        os.close();

        InputStream is = fs.open(new Path("/user/oslab/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
        System.out.println("success!");
    }
}