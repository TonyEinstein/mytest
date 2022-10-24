package com.mytest.exam_six_SumMony;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 12/6/19.
 */
public class SMoneyMapper extends Mapper<LongWritable,Text,Text,Text> {
     /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
      *
     EMPNO	    INT	        员工ID
     ENAME	    VARCHAR(10)	员工名称
     JOB	    VARCHAR(9)	员工职位
     MGR	    INT	        直接领导的员工ID
     HIREDATE	DATE	雇佣时间
     SAL	    INT	工资
     COMM	    INT	奖金
     DEPTNO		    部门号
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueS = value.toString();
        String[] valueA = valueS.split(",");
        if (valueA.length == 8){
            String deptno = valueA[7];
            String sal = valueA[5];
            context.write(new Text(deptno),new Text(sal));
        }
        if (valueA.length == 7){
            String deptno = valueA[6];
            String sal = valueA[5];
            context.write(new Text(deptno),new Text(sal));
        }
        if (valueA.length ==6){
            String deptno = valueA[5];
            String sal = valueA[4];
            context.write(new Text(deptno),new Text(sal));

        }

    }
}
