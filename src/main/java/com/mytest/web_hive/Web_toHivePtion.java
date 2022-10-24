package com.mytest.web_hive;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ua07 on 11/22/19.
 */
public class Web_toHivePtion extends Partitioner<NullWritable,Text> {

    @Override
    public int getPartition(NullWritable nullWritable, Text text, int numPartition) {
        String[] tmpL = text.toString().split(",");
        if (tmpL.length == 5){
            return 1 % numPartition;
        }else{
            return 2% numPartition;
        }
    }
}
