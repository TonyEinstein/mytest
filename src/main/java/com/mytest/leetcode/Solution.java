package com.mytest.leetcode;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by ua07 on 11/13/19.
 */
public class Solution {
    public static void main(String[] arg) throws Exception {
        int[] s= {1,2,3};
        ArrayList ai = new ArrayList();
        for (int i=0; i<s.length; i++){
            ai.add(s[i]);
        }
        ai.add(4);
        Iterator iterator_ArrayLsit = ai.iterator();
        while (iterator_ArrayLsit.hasNext()) {
            Object obj = iterator_ArrayLsit.next();
            System.out.println(obj);
        }

    }
}
