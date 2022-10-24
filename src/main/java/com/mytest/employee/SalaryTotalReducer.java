package com.mytest.employee;


import com.mytest.employee.SalaryEmployee;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


/**
 * Created by ua07 on 9/24/19.
 */
public class SalaryTotalReducer extends Reducer<IntWritable,SalaryEmployee,IntWritable,Text> {
    @Override
    protected void reduce(IntWritable k3, Iterable<SalaryEmployee> v3, Context context) throws IOException, InterruptedException {
//        int total = 0;
//        for (SalaryEmployee e:v3) {
//            total = total+e.getSal();
//        }
        for (SalaryEmployee e:v3){
            int Sal = e.getSal();
            String sal_String = "high";
            if (Sal < 1500){          // in 1
                sal_String = "low";

            }
            else if (Sal >= 3000){
//                String sal_String = Integer.toString(Sal);
                sal_String = "high";
            }
            else {
                sal_String = "center";
            }

            context.write(k3,new Text(sal_String));
        }

//        context.write(k3,new IntWritable(total));
    }
}
