package com.mytest.CDnow;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ua07 on 12/20/19.
 */
public class CDMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s1 = value.toString().replaceFirst("\\s+","");
        String s = s1.replaceAll("\\s+",",");
        String[] valueArrays = s.split(",");
        if(valueArrays.length == 4){
            if (valueArrays[0].length()==5 && valueArrays[1].length()==8){
                int y1 = Integer.parseInt(valueArrays[1].substring(0,4));
                if (y1==1997 || y1==1998){
                    int y2 = Integer.parseInt(valueArrays[1].substring(4,6));
                    if (y2 >0 && y2 <13){
                        int y3 = Integer.parseInt(valueArrays[1].substring(6,8));
                        if (y3 >0 && y3 <=31){
                            String order_dt;
                            if (y1 == 1997){
                                order_dt = "97";
                            }else {
                                order_dt = "98";
                            }
                            String month = String.valueOf(y2);
                            String rst = valueArrays[0]+","+order_dt+","+month+","+valueArrays[2]+","+valueArrays[3];
                            context.write(NullWritable.get(),new Text(rst));
                        }

                    }
                }

            }

        }

    }

    protected boolean judges(String[] s){
//        00001 19970101  1   11.77
        return true;

    }
}
