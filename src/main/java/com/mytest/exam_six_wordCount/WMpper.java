package com.mytest.exam_six_wordCount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 12/6/19.
 */
public class WMpper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] woldArray = value.toString().split(" ");
        for (String s: woldArray) {
            context.write(new Text(s),new IntWritable(1));
        }
    }
}
