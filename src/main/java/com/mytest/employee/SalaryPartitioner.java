package com.mytest.employee;


import com.mytest.employee.SalaryEmployee;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ua07 on 10/7/19.
 */
public class SalaryPartitioner extends Partitioner<IntWritable,SalaryEmployee> {
    @Override
    public int getPartition(IntWritable k2,SalaryEmployee v2,int numPartition){
        if (v2.getSal() < 1500){
//            String sal1 = Integer.toString(v2.getSal());
//            sal1 = "low";
            // in 1
            return 1%numPartition;
        }
        else if (v2.getSal() >= 3000){
//            String sal3 = Integer.toString(v2.getSal());
//            sal3 = "high";
            return 2%numPartition;
        }
        else {
//            String sal2 = Integer.toString(v2.getSal());
//            sal2 = "center";
            return 3%numPartition;
        }
    }
}
