package com.mytest.employee;


import com.mytest.employee.SalaryEmployee;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 9/24/19.
 */
public class SalaryTotalMapper extends Mapper<LongWritable,Text,IntWritable,SalaryEmployee> {
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException {
        String data = value1.toString();

        String[] words = data.split(",");
        SalaryEmployee e = new SalaryEmployee();
        e.setEmpno(Integer.parseInt(words[0]));
        e.setEname(words[1]);
        e.setJob(words[2]);

        try{
            e.setMgr(Integer.parseInt(words[3]));
        }catch (Exception ex){
            e.setMgr(-1);
        }
        e.setHiredate(words[4]);
        e.setSal(Integer.parseInt(words[5]));
        try {
            e.setComm(Integer.parseInt(words[6]));
        }catch (Exception ex){
            e.setComm(0);
        }
        e.setDeptno(Integer.parseInt(words[7]));

        context.write(new IntWritable(e.getSal()),e);
    }

}
