package com.mytest.web_hive;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by ua07 on 11/22/19.
 */
public class Web_toHiveMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri,conf,"ua07");
        Path outputpath = new Path(args[1]);
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
            System.out.println("Deleted existsPath OK!");
        }

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job,new Path(args[0]));


        job.setJarByClass(Web_toHiveMain.class);

        job.setMapperClass(Web_toHiveMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);



        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
