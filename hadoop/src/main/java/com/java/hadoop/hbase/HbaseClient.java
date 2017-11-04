package com.java.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by sgr on 2017/11/4.
 */
public class HbaseClient {
    private HBaseAdmin hBaseAdmin;
    public boolean init(){
        Configuration configuration = new Configuration();
        //hbaseadmin通过zk进行访问，不需要将配置文件加载
        configuration.set("hbase.zookeeper.quorum","node1,node2,node3");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            return false;
        }
        return true;
    }


    public HTable createTable(String tableName, String columnFamily){
        try {
            if (hBaseAdmin.tableExists(tableName)){
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
            }
        } catch (IOException e) {
            return null;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //设置列族（建议设置列族个数为：1-3,这边一个就够了）
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        //设置读缓存
        hColumnDescriptor.setBlockCacheEnabled(true);
        //设置加载到内存
        hColumnDescriptor.setInMemory(true);
        //设置最大版本数
        hColumnDescriptor.setMaxVersions(1);
        //添加列族
        hTableDescriptor.addFamily(hColumnDescriptor);
        try {
            hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            return null;
        }
        TableName tn = TableName.valueOf(tableName);
        try {
            return new HTable(hBaseAdmin.getConfiguration(),tn);
        } catch (IOException e) {
            return null;
        }
    }

    public void closeTable(HTable table){
        if (table != null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(){
        if (hBaseAdmin != null){
            try {
                hBaseAdmin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
