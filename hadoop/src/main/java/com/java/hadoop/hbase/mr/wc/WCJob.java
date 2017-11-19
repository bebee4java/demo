package com.java.hadoop.hbase.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * MR单词统计
 * 从hdfs上取数据统计结果写入hbase表中
 * 通过MR实现多线程对Hbase的读写操作
 * Created by sgr on 2017/10/17/017.
 */
public class WCJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hdfs安全认证基于用户名匹配
        //客户端想上传文件用户名必须一致
        //1.可以修改客户端的用户名
        //2.不修改客户端用户名，代码中设置HADOOP_USER_NAME变量
        System.setProperty("HADOOP_USER_NAME","sgr");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://node2:8020");//active状态的NN
//        configuration.set("yarn.resourcemanager.hostname", "node2");//active状态的RSM

        configuration.set("hbase.zookeeper.quorum","node1,node2,node3");
//        configuration.set("mapred.jar","E:\\MR\\wc\\wc.jar");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(WCJob.class);
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path("/sgr/wc/input"));

        //hbase reduce
        TableMapReduceUtil.initTableReducerJob(
                "wc", // reduce输出到hbase的目标表
                WCTableReducer.class, // reducer class
                job);

        boolean result = job.waitForCompletion(true);

        if (result){
            System.out.println("WCJob done!");
        }
    }
}
