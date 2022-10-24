package com.mytest.whl;


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
 * Created by ua07 on 11/29/19.
 */
public class Whl_Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri,conf,"ua07");
//        Path outputpath = new Path(args[1]);
        Path outputpath = new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/Hadoop实验五_output");
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
            System.out.println("Is existsPath \n--->>>\n Deleted existsPath OK!");
        }



        Job job = Job.getInstance(conf);
        job.setJarByClass(Whl_Main.class);

        job.setMapperClass(Whl_Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m0:8020/user/ua07/hadoop部署实战/Hadoop实验五"));
        FileOutputFormat.setOutputPath(job, outputpath);

        job.waitForCompletion(true);
        System.out.println("finshed work");
    }
}
