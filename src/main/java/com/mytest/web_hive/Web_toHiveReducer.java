package com.mytest.web_hive;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ua07 on 11/22/19.
 */
public class Web_toHiveReducer extends Reducer<NullWritable,Text,Text,LongWritable>{
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
