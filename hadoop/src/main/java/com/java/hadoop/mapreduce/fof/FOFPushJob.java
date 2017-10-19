package com.java.hadoop.mapreduce.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 按名称升序，亲密度降序排序，推送亲密度最高的好友
 * Created by sgr on 2017/10/18.
 */
public class FOFPushJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hdfs安全认证基于用户名匹配
        //客户端想上传文件用户名必须一致
        //1.可以修改客户端的用户名
        //2.不修改客户端用户名，代码中设置HADOOP_USER_NAME变量
        System.setProperty("HADOOP_USER_NAME","sgr");

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FOFPushJob.class);
        job.setMapperClass(FoFPushMapper.class);
        job.setMapOutputKeyClass(Friend.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(FOFPushReduce.class);

        job.setSortComparatorClass(FOFPushSort.class);
        job.setGroupingComparatorClass(FOFPushGroup.class);

        FileInputFormat.addInputPath(job,new Path(""));

        Path outPath = new Path("");
        //如果存在先删除，保证MR程序正常
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        boolean result = job.waitForCompletion(true);

        if (result){
            System.out.println("FOFPushJob done!");
        }
    }

}
