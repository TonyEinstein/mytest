package com.mytest.city;


import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;


/**
 * Created by ua07 on 9/29/19.
 */
public class CityMapper extends TableMapper<Text,IntWritable> {
    /**
     *
     * @param key = row_key
     * @param value = a row
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,InterruptedException{


        String city = Bytes.toString(value.getValue(Bytes.toBytes("address"),Bytes.toBytes("city")));
        context.write(new Text(city),new IntWritable(1));


    }
}