package com.mytest.web_hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by ua07 on 11/22/19.
 */
public class Web_Main {

    public static class Web_Mapper extends Mapper<LongWritable,Text,NullWritable,Text> {
        //
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            /**
             * [0] : phone+
             * [1] : url+
             * [2] : up
             * [3] : down
             */
            //http
            if (values.length ==4){
                int flow = Integer.parseInt(values[2])+Integer.parseInt(values[3]);
                String flows = Integer.toString(flow);
                String phone7= values[0].substring(0,7);
                //phone7 , phone , url , flows,phone3;-------------5
                String newValue = phone7+","+values[0]+","+values[1]+","+flows+","+values[0].substring(0,3);
                context.write(NullWritable.get(),new Text(newValue));
            }

            /**
             * 0:手机号前缀，1:手机号段+，2:手机号码对应的省份+， 3:城市+，4:运营商+，5:邮编， 6:区号， 7:行政划分代码
             */
            //phone
            if (values.length==8){
                // 1 2 3 4 ----------------4
                String rst8 = values[1]+","+values[2]+","+values[3]+","+values[4];
                context.write(NullWritable.get(),new Text(rst8));
            }
        }
    }

    /**
     * doing : Partition
     */
    public static class Web_Partition extends Partitioner<NullWritable,Text> {

        /**
         *
         * @param nullWritable
         * @param text--value
         * @param numPartition
         * @return
         */
        @Override
        public int getPartition(NullWritable nullWritable, Text text, int numPartition) {
            String[] tmpL = text.toString().split(",");
            if (tmpL.length == 4){ //http
                return 1%numPartition;
            }else{
                return 2%numPartition;
            }

        }
    }

    /**
     * doing : notThing
     */
    public static class Web_Reducer extends Reducer<NullWritable,Text,Text,LongWritable> {
        /**
         *
         * @param key ---NUll
         * @param values --- record
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://xdata-m0:8020");
        FileSystem fs = FileSystem.get(uri,conf,"ua07");
//        Path outputpath = new Path(args[1]);
        Path outputpath = new Path("hdfs://xdata-m0:8020/user/ua07/web_out/");
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
            System.out.println("Deleted existsPath OK!");
        }

        Job job = Job.getInstance(new Configuration());

//        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setInputPaths(job,new Path("hdfs://xdata-m0:8020/user/ua07/web_toHive/"));
        System.out.println("input is OK!");

        job.setJarByClass(Web_Main.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(Web_Partition.class);

        job.setMapperClass(Web_Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Web_Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path("hdfs://xdata-m0:8020/user/ua07/web_out/"));
        System.out.println("output is OK!");
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("job is OK!");

    }
}
