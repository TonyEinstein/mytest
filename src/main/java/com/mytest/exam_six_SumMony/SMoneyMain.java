package com.mytest.exam_six_SumMony;


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
public class SMoneyMain {
    /**
     * emp.csv
     * @param arg
     * @throws Exception
     */
    public static void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();

        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri,conf,"ua07");
        Path outputpath = new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/emp_output");
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
            System.out.println("Deleted existsPath OK!");
        }

        Job job = new Job(conf);
//        /user/ua07/hadoop部署实战/exam_six/emp.csv
        FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/exam_six/emp.csv"));

        job.setJarByClass(SMoneyMain.class);

        job.setMapperClass(SMoneyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SMoneyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, outputpath);
        job.waitForCompletion(true);
    }
}
