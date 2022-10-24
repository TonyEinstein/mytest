package com.mytest.city;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


/**
 * Created by ua07 on 9/29/19.
 */
public class CityMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","xdata-m0,xdata-m1,xdata-m2");

        //create job
        Job job = Job.getInstance(conf);
        job.setJarByClass(CityMain.class);

        // def Scan
        Scan scan = new Scan();

        // scan data column
        scan.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"));

        //map,input is table
        TableMapReduceUtil.initTableMapperJob("member777",scan,CityMapper.class, Text.class, IntWritable.class,job);


        //reduce output is table result
        TableMapReduceUtil.initTableReducerJob("rst007",CityReducer.class,job);



//        String path1 = args[0];
//        TableMapReduceUtil.initTableMapperJob(path1,scan,CityMapper.class, Text.class, IntWritable.class,job);
//        String path2 = args[1];
//        TableMapReduceUtil.initTableReducerJob(path2,CityReducer.class,job);

        job.waitForCompletion(true);

    }
}
