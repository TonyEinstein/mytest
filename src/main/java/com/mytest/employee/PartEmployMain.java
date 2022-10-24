package com.mytest.employee;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by ua07 on 10/7/19.
 */
public class PartEmployMain {
    public static void main(String[] arg) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PartEmployMain.class);

        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SalaryEmployee.class);

        job.setPartitionerClass(SalaryPartitioner.class);
        job.setNumReduceTasks(3);


        job.setReducerClass(SalaryTotalReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SalaryEmployee.class);


        FileInputFormat.setInputPaths(job,new Path(arg[0]));
        FileOutputFormat.setOutputPath(job,new Path(arg[1]));

        job.waitForCompletion(true);
    }
}
