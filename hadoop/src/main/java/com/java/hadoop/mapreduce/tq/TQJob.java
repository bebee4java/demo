package com.java.hadoop.mapreduce.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 找出每个年月里面温度最高的两天
 * Created by sgr on 2017/10/18/018.
 */
public class TQJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hdfs安全认证基于用户名匹配
        //客户端想上传文件用户名必须一致
        //1.可以修改客户端的用户名
        //2.不修改客户端用户名，代码中设置HADOOP_USER_NAME变量
        System.setProperty("HADOOP_USER_NAME","sgr");

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(TQJob.class);
        job.setMapperClass(TQMapper.class);
        job.setMapOutputKeyClass(WeatherRecord.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(TQReduce.class);

        job.setPartitionerClass(TQPartition.class);
        job.setSortComparatorClass(TQSort.class);
        job.setGroupingComparatorClass(TQGroup.class);

        job.setNumReduceTasks(3);//指定reduce个数

        FileInputFormat.addInputPath(job,new Path(""));

        Path outPath = new Path("");
        //如果存在先删除，保证MR程序正常
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        boolean result = job.waitForCompletion(true);

        if (result){
            System.out.println("TQJob done!");
        }
    }
}
