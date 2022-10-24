package com.mytest.demo_sum_count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 9/27/19.
 */
public class DemoMapper extends Mapper<LongWritable,Text,Text,Text> {
    /**
     *
     * @param key1
     * @param value1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException {
        String data = value1.toString();
        String[] words = data.split(",");

        int salve_num = Integer.parseInt(words[5]);
        float salve_money = Float.parseFloat(words[6]);

        String colls = Integer.toString(salve_num)+"/"+Float.toString(salve_money);
        context.write(new Text(words[2].substring(0,4)),new Text((colls)));
    }


}
