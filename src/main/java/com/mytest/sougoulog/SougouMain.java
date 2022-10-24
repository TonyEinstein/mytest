package com.mytest.sougoulog;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


/**
 * Created by ua07 on 10/29/19.
 */
public class SougouMain {

    public static class SougouMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        /**
         *
         * @param key:
         * @param value:
         * @param context:
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = new String(value.getBytes(),0,value.getLength(),"GBK");
            String[] demo = data.split("\\s+");
            if (demo.length != 6){
                return;
            }
            String splitdata = data.replaceAll("\\s+",",");
            context.write(new Text(splitdata),NullWritable.get());

        }
    }

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
//        FileInputFormat.setInputPaths(job,new Path("/user/ua07/SogouQ.reduced"));

        job.setJarByClass(SougouMain.class);

        job.setMapperClass(SougouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


//        FileOutputFormat.setOutputPath(job,new Path("/user/ua07/SogouOut"));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}


