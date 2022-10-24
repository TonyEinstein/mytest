package com.mytest.phone_MR;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ue50 on 11/13/19.
 */
public class PhoneReducer extends Reducer<Text, Text, Text, LongWritable>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        List<String> valueList = new LinkedList<String>();//基于链表的动态数组

        //Map是一种把键对象和值对象映射的集合,TreeMap是一个有序的key-value集合，它是通过红黑树实现的,TreeMap中的元素默认按照key的自然排序排列
        Map<String, Long> residenceTimeMap = new TreeMap<String, Long>();

        for(Text value : values)
        {
            String item = value.toString();
            valueList.add(item);//"基站位置-访问时间"的集合
        }

        if(valueList == null)
        {

            return;
        }

//        if (valueList.size() == 1){
//
//        }

        //Comparator是比较器
        //Collections.sort()方法中的自定义比较器,根据比较器的实现逻辑对valueList进行排序
        Collections.sort(valueList, new Comparator<String>() {//匿名内部类,也就是没有名字的内部类
            @Override
            //重写比较器中的比较方法：compare方法
            public int compare(String o1, String o2) {
                o1 = o1.split("-")[1];
                o2 = o2.split("-")[1];
                return o1.compareTo(o2);//根据访问时间对valueList排序,第一个参数.compareTo(第二个参数)升序
            }
        });


        for(int i = 0;i < valueList.size()-1; i++)
        {
            String station = valueList.get(i).split("-")[0];//基站位置
            String time1 = valueList.get(i).split("-")[1];//访问时间
            String time2 = valueList.get(i + 1).split("-")[1];

            //对日期/时间进行格式化,HH:24小时制
            DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
            //Date对象用于处理日期与时间
            Date date1 = null;
            Date date2 = null;
            try{
                date1 = dateFormat.parse(time1);//parse():把String型的字符串转换成特定格式的Date类型
                date2 = dateFormat.parse(time2);
            }catch (ParseException e)
            {
                e.printStackTrace();
            }

            if(date1 == null || date2 == null)
                return;

            //date1.before(date2),当date1小于date2时，返回TRUE，当大于等于时，返回false；
            if(date1.before(date2))
            {
                long time = date2.getTime() - date1.getTime();//getTime方法返回的是毫秒数

                Long count = residenceTimeMap.get(station);//返回key关联的值,没有值返回null
                if(count == null)
                {
                    residenceTimeMap.put(station, time);//<基站位置,停留时间>
                }
                else
                {
                    residenceTimeMap.put(station, count + time);//将停留时间累积
                }
            }
        }

        valueList = null;

        //TreeMap的keySet():以升序返回一个具有TreeMap键的Set视图
        Set<String> keySet = residenceTimeMap.keySet();//keySet:<基站位置>
        for(String mapKey : keySet)
        {
            long minute = residenceTimeMap.get(mapKey);//停留时间毫秒
            minute = minute/1000/60;//分钟
            //minute = minute/1000;//秒

            context.write(new Text(key +"\t" + mapKey +"\t"), new LongWritable(minute));
        }

        residenceTimeMap = null;
    }
}
