package com.mytest.phone_MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * Created by ue50 on 11/13/19.
 */
public class PhoneMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private int[] timeRangeList;
    @Override
    //setup()被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作
    protected void setup(Context context) throws IOException,InterruptedException
    {
        //Configuration是作业的配置信息类,通过Configuration可以实现在多个mapper和多个reducer任务之间共享信息
        Configuration configuration = context.getConfiguration();

        //get(String name)根据配置项的键name获取相应的值
        String timeRange = configuration.get("timeRange");//运行时传入的时间段,比如“09-18-24”
        String[] timeRangeString = timeRange.split("-");

        timeRangeList = new int[timeRangeString.length];
        for(int i = 0; i < timeRangeString.length;i++)
        {
            //timeRangeList数组保存传入的时间,如:09、18、24
            timeRangeList[i] = Integer.parseInt(timeRangeString[i]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String values[] = value.toString().split("\\s+");//对一条记录"用户标识 设备标识   基站位置 通讯的时间"按空格拆分
        String userId = values[0];//用户标识
        String baseStation = values[2];//基站位置
        String timeString = values[4];//访问时间,如:21:55:37

        String[] times = timeString.split(":");//对访问时间按':'拆分
        int hour = Integer.parseInt(times[0]);//小时

        //startHour、endHour时间段的起止时间
        int startHour = 0;
        int endHour = 0;
        for(int i = 0; i < timeRangeList.length; i++)
        {
            if(hour < timeRangeList[i])
            {
                if(i == 0)
                {
                    startHour = 0;
                }
                else
                {
                    startHour = timeRangeList[i-1];
                }
                endHour = timeRangeList[i];
                break;
            }
        }

        if(endHour == 0)
            return;

        //k2:用户标识  时间段  v2:基站位置-访问时间
        context.write(new Text(userId + "\t" + startHour + "-" + endHour + "\t"), new Text(baseStation + "-" + timeString));
    }
}
