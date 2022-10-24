package com.mytest.demo_sum_count;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;

/**
 * Created by ua07 on 9/27/19.
 */
public class DemoReducer extends Reducer<Text,Text,Text,Text> {
    /**
     *
     * @param k3
     * @param v3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text k3,Iterable<Text> v3,Context context) throws IOException,InterruptedException{
        int salve_num_sum = 0;
        float salve_money_sum = 0;

        for(Text x:v3){
            String colls = x.toString();
            String[] collArray = colls.split("/");

            int salve_num = Integer.parseInt(collArray[0]);
            float salve_money = Float.parseFloat(collArray[1]);
            salve_num_sum = salve_num_sum + salve_num;
            salve_money_sum = salve_money_sum + salve_money;
        }

        String salve_Num = Integer.toString(salve_num_sum);
        String salve_Money = Float.toString(salve_money_sum);
        String collss = " 销售笔数:"+salve_Num+" 销售额:"+salve_Money;
        context.write(k3,new Text(collss));

    }
}
