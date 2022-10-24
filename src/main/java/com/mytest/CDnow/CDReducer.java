package com.mytest.CDnow;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ua07 on 12/20/19.
 */
public class CDReducer extends Reducer<NullWritable,Text,NullWritable,Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
