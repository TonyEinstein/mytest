package com.mytest.exam_six_name;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 12/6/19.
 *
 */
public class NameMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueS = value.toString();
        String[] valueA = valueS.split(",");
        if (valueA.length == 8){
            String deptno = valueA[7];
            String namer = valueA[2]+"1";
            context.write(new Text(deptno),new Text(namer));
        }
        if (valueA.length == 7){
            String deptno = valueA[6];
            String namer = valueA[2]+"1";
            context.write(new Text(deptno),new Text(namer));
        }
        if (valueA.length ==6){
            String deptno = valueA[5];
            String namer = valueA[2]+"1";
            context.write(new Text(deptno),new Text(namer));
        }
        if (valueA.length ==3){
            String deptno = valueA[0];
            // bu meng hao
            String dname = valueA[1]+"2";
            context.write(new Text(deptno),new Text(dname));
        }
    }
}
