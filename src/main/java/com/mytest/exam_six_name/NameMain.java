package com.mytest.exam_six_name;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by ua07 on 12/6/19.
 */
public class NameMain {
    public static void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri, conf, "ua07");
        Path outpath = new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/name_output");
        if (fs.exists(outpath)) {
            fs.delete(outpath);
            System.out.println("delete is OK!!!");
        }
        Job job = new Job(conf);
        job.setJarByClass(NameMain.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/name/"));

        //map
        job.setMapperClass(NameMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //reduce
        job.setReducerClass(NameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(job,outpath);

        job.waitForCompletion(true);
    }
}
