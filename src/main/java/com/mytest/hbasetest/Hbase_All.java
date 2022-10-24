package com.mytest.hbasetest;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;

import java.util.List;

/**
 * Created by ua07 on 10/15/19.
 */


public class Hbase_All {

    /**
     *
     * @param admin
     * @param tablename
     * @throws IOException
     */
    private static void createTable(HBaseAdmin admin, String tablename) throws IOException {
        HTableDescriptor tdesc=new HTableDescriptor(tablename); //table name

        tdesc.addFamily(new HColumnDescriptor("id"));
        tdesc.addFamily(new HColumnDescriptor("info"));
        tdesc.addFamily(new HColumnDescriptor("address"));

        admin.createTable(tdesc);
        System.out.println("create OK");
        admin.close();
    }

    /**
     *
     * @param conf
     * @param tableName
     * @throws IOException
     */
    protected static void addData(Configuration conf,String tableName) throws IOException {
        HTable table = new HTable(conf,tableName);
        List<Put> putList = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes("Rain"));
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes(""), Bytes.toBytes("31"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("28"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1990-05-01"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("industry"), Bytes.toBytes("architect"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("Shenzhen"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("China"));
        putList.add(put);

        put = new Put(Bytes.toBytes("Sariel"));
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes(""), Bytes.toBytes("21"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("26"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1992-05-09"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("industry"), Bytes.toBytes("it"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("Shanghai"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("China"));
        putList.add(put);

        put = new Put(Bytes.toBytes("Elvis"));
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes(""), Bytes.toBytes("22"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("26"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1991-09-14"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("industry"), Bytes.toBytes("it"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("Beijing"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("China"));
        putList.add(put);

        table.put(putList);
        System.out.println("add data OK");
        table.close();

    }

    /**
     *
     * @param conf
     * @param tablename
     * @param rownum
     * @param infoname
     * @param colunmnName
     * @param value
     * @throws IOException
     */
    private static void insert(Configuration conf,String tablename,String rownum,String infoname,String colunmnName,String value) throws IOException {
        HTable table = new HTable(conf,tablename);

        Put put =  new Put(Bytes.toBytes(rownum));
        put.addColumn(
                Bytes.toBytes(infoname),
                Bytes.toBytes(colunmnName),
                Bytes.toBytes(value)
                );
        table.put(put);
        System.out.println("insert OK");
        table.close();
    }


    /**
     *
     * @param conf
     * @param tablename
     * @param rownum
     * @param infoname
     * @param columnname
     * @throws IOException
     */
    private static void get(Configuration conf,String tablename,String rownum,String infoname,String columnname) throws IOException {
        HTable table = new HTable(conf,tablename);
        Get get = new Get(Bytes.toBytes(rownum));
        Result record = table.get(get);

        String name = Bytes.toString(record.getValue(Bytes.toBytes(infoname),Bytes.toBytes(columnname)));
        System.out.println(name);
        System.out.println("get ok");
        table.close();

    }

    /**
     *
     * @param conf
     * @param tablename
     * @param rownum
     * @param infoname
     * @param columnnames
     * @throws IOException
     */
    private static void scan(Configuration conf,String tablename,String rownum,String infoname,String columnnames) throws IOException {
        HTable table = new HTable(conf,tablename);
        Scan scanner = new Scan(); // as select * from tablename
//        scanner.setFilter(filter); // run filter
        ResultScanner rs = table.getScanner(scanner); //excute search
        String[] columns = columnnames.split("-");
        for (Result r:rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes(infoname),Bytes.toBytes(columns[0])));
            String age = Bytes.toString(r.getValue(Bytes.toBytes(infoname),Bytes.toBytes(columns[1])));
            System.out.println(name+"  "+age);
        }
        System.out.println("scan OK");
        table.close();

    }

    /**
     *
     * @param conf
     * @param tablename
     * @throws IOException
     */
    private static void delete(Configuration conf,String tablename) throws IOException {
        HBaseAdmin client = new HBaseAdmin(conf);
        client.disableTable(tablename);
        client.deleteTable(tablename);
        System.out.println("delete OK");
        client.close();
    }


    /**
     *
     * @param arg
     * @throws Exception
     */
    public static void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","xdata-m0,xdata-m1,xdata-m2");
        HBaseAdmin admin = new HBaseAdmin(conf);

//        Hbase_All.createTable(admin,"member777");
//        Hbase_All.addData(conf,"member777");
//        Hbase_All.scan(conf,"member7777");


//        Hbase_All.delete(conf,"member777");

    }
}
