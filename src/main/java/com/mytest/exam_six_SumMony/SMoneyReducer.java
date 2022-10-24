package com.mytest.exam_six_SumMony;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ua07 on 12/6/19.
 */
public class SMoneyReducer extends Reducer<Text, Text,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumS = 0;
        for (Text vText : values){
            String tmp = vText.toString();
            int tmpCount = Integer.parseInt(tmp);
            sumS += tmpCount;
        }
        context.write(key,new IntWritable(sumS));
    }
}
