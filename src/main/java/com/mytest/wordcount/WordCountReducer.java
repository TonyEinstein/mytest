package com.mytest.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ua07 on 9/20/19.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    /**
     *
     * @param k3
     * @param v3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text k3,Iterable<IntWritable> v3,Context context)
            throws IOException,InterruptedException{
        int total = 0;
        for (IntWritable v:v3){
            total += v.get();
        }
        context.write(k3,new IntWritable(total));
    }
}
