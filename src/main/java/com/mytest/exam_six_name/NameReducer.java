package com.mytest.exam_six_name;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ua07 on 12/6/19.
 */
public class NameReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String namer = "";
        String dname = "";
        for (Text value : values) {
            String v = value.toString();
            if (v.contains("1")){
                namer += v.substring(0,v.length()-1)+",";
            }
            if (v.contains("2")){
                dname += v.substring(0,v.length()-1)+",";
            }
        }

        String[] dnameArray = dname.split(",");
        String dnameRst =dnameArray[0];

        context.write(new Text(dnameRst),new Text(namer));
    }
}
