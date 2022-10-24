package com.mytest.phone_MR;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ue50 on 11/13/19.
 */
public class PhoneMain
{
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException
    {
        //String.equals()比较字符串的值是否相同
        if(args == null || "0".equals("07-15-24"))
        {
            throw new RuntimeException("argument is not right!");
        }

        //Configuration是作业的配置信息类
        Configuration configuration = new Configuration();
        //set(String name, String value)设置配置项
        configuration.set("timeRange", "07-15-24");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PhoneMain.class);

        job.setMapperClass(PhoneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setReducerClass(PhoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://xdata-m0:8020/user/ua07/phone/pos.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://xdata-m0:8020/user/ua07/phone/out1"));

//        FileInputFormat.setInputPaths(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
