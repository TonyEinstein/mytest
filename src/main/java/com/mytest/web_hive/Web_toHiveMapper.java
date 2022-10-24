package com.mytest.web_hive;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 11/22/19.
 */
public class Web_toHiveMapper extends Mapper<LongWritable,Text,NullWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = value.toString().split("\\s+");

        if (values.length !=4 || values.length != 8){
            return;
        }

        /**
         * [0] : phone
         * [1] : url
         * [2] : up
         * [3] : down
         */
        //http
        if (values.length ==4){
            int flow = Integer.parseInt(values[2])+Integer.parseInt(values[3]);
            String flows = Integer.toString(flow);

            String phone7 = values[0].substring(0,7);
            String newValue;
            newValue = phone7+","+values[1]+","+values[2]+","+values[3]+","+flows;
            context.write(NullWritable.get(),new Text(newValue));

        }

        /**
         * 0:手机号前缀，1:手机号段，2:手机号码对应的省份+， 3:城市+，4:运营商+，5:邮编， 6:区号， 7:行政划分代码
         */
        //phone
        if (values.length==8){
            context.write(NullWritable.get(),new  Text(values[1]+","+values[2]+","+values[3]+","+values[4]));
        }


    }
}
