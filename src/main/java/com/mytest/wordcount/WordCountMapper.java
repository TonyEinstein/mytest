package com.mytest.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 9/20/19.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     *
     * @param key1
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key1,Text value,Context context)
            throws IOException,InterruptedException{
        String data = value.toString();
        String[] words = data.split(" ");

        for(String w:words){
            context.write(new Text(w),new IntWritable(1));
        }

    }
}
