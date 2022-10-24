package com.mytest.whl;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ua07 on 11/29/19.
 */
public class Whl_Mapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * input:
         * files:  访问ip  访问时间  访问资源  访问状态  访问流量
         * values:  {27.19.74.143 - - [30/May/2013:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127}
         *
         * output(return): ip,date,time,isRegister
         */
        String data = value.toString();
        String[] dataArray = data.split(" - - ");
        String[] dataArray_last = dataArray[1].split(" ");
        /**
         * Array = {"[30/May/2013:17:38:20","+0800]",'"GET',"/static/image/common/faq.gif",'HTTP/1.1"','200','1127'}
         */
        if (dataArray_last.length !=7){
            return;
        }else{
            String ip = dataArray[0];
            boolean isRegister = false;
            String dateStr = dataArray_last[0].substring(1,dataArray_last[0].length());
            if (dataArray_last[2].substring(1,dataArray_last[2].length()) == "GET"){
                if (dataArray_last[3].split("/")[0]=="static" | dataArray_last[3].split("/")[0]=="uc_server"){
                return;
                }else{
                    // is GET but not "static"|"uc_server"
                    try {
                        String[] datearray = getDateArray(dateStr); //date time
                        String url = dataArray_last[3];
                        if (url.contains("member.php?mod=register")){
                            isRegister = true;
                        }
                        String text = ip+","+datearray[0]+","+datearray[1]+","+url+","+isRegister;
                        context.write(NullWritable.get(),new Text(text));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
            else{// not GET
                try {
                    String[] datearray = getDateArray(dateStr);
                    String url = dataArray_last[3];
                    //判断是否是新注册用户: includeing "member.php?mod=register"
                    if (url.contains("member.php?mod=register")){
                        isRegister = true;
                    }
                    String text = ip+","+datearray[0]+","+datearray[1]+","+url+","+isRegister;
                    context.write(NullWritable.get(),new Text(text));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private static String[] getDateArray(String dateStr) throws ParseException {
        DateFormat dateFormat1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss");

        Date dates = dateFormat1.parse(dateStr);
        dateStr = dateFormat2.format(dates); //format---date

        String[] datearray;
        datearray = dateStr.split(",");
        return datearray;
    }


}
