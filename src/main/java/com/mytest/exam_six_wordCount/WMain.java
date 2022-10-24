package com.mytest.exam_six_wordCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by ua07 on 12/6/19.
 */
public class WMain {
    public static void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();

        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri,conf,"ua07");
        Path outputpath = new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/word_output");
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
            System.out.println("Deleted existsPath OK!");
        }

        Job job = new Job(conf);
//        /user/ua07/hadoop部署实战/exam_six/word_input.txt
        FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/word_input.txt"));

        job.setJarByClass(WMain.class);

        job.setMapperClass(WMpper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        System.out.println("map is ok");

        job.setReducerClass(WReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        System.out.println("reduce is ok");

        FileOutputFormat.setOutputPath(job, outputpath);
        job.waitForCompletion(true);
        System.out.println("OK");
    }
}
