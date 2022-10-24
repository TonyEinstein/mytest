package com.mytest.CDnow;



/**
 * Created by ua07 on 12/20/19.
 */
public class Test {
    public static void main(String[] arg) throws Exception {
        String s = " 00001 19970101  1   11.77";
        String valueArray1 = s.replaceFirst("\\s+","");
        String valueArray = valueArray1.replaceAll("\\s+",",");
        String[] valueArrays = valueArray.split(",");

//        for (int i = 0; i < valueArrays.length;i++){
//            System.out.println(valueArrays[i]);
//        }

        int y1 = Integer.parseInt(valueArrays[1].substring(6,8));
        System.out.println(y1);
        System.out.println(valueArrays.length);
    }
}
