package com.mytest.city;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;


/**
 * Created by ua07 on 9/29/19.
 */

//tablename : member777
//tablename: rst007

public class CityReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
    /**
     *
     * @param k3 = row key
     * @param v3 = row key
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text k3,Iterable<IntWritable> v3,Context context) throws IOException,InterruptedException{
        int sum_count=0;
        for (IntWritable i:v3) {
            sum_count = sum_count+i.get();
        }

//        int sum_count = 1;

        Put put = new Put(Bytes.toBytes(k3.toString()));
        put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(sum_count)));

        context.write(new ImmutableBytesWritable(Bytes.toBytes(k3.toString())),put);

    }
}
