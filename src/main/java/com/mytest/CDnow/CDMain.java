package com.mytest.CDnow;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by ua07 on 12/20/19.
 */
public class CDMain {
    public static void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();

        URI uri = new URI("hdfs://xdata-m0:8020");

        FileSystem fs = FileSystem.get(uri,conf,"ua07");

        Path outputpath = new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/CD_output");

        if(fs.exists(outputpath)){

            fs.delete(outputpath,true);

            System.out.println("Deleted existsPath OK!");
        }

        Job job = new Job(conf);

        job.setJarByClass(CDMain.class);

//        "/user/ua07/hadoop部署实战/CDNOW_master.txt"
        FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/CDNOW_master.txt"));

        job.setMapperClass(CDMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        System.out.println("mapper is OK");

        job.setReducerClass(CDReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.out.println("reducer is OK");

        FileOutputFormat.setOutputPath(job,outputpath);

        job.waitForCompletion(true);
        System.out.println("CDmain is OK");
    }
}
