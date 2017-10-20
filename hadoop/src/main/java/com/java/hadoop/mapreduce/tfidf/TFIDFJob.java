package com.java.hadoop.mapreduce.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TFIDFJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hdfs安全认证基于用户名匹配
        //客户端想上传文件用户名必须一致
        //1.可以修改客户端的用户名
        //2.不修改客户端用户名，代码中设置HADOOP_USER_NAME变量
        System.setProperty("HADOOP_USER_NAME","sgr");

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setJobName("TFIDFJob");
        job.setJarByClass(TFIDFJob.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TFIDFReduce.class);


        //将文档总数加载到内存
        job.addCacheFile(new Path("/sgr/tfidf/output/TFAndDCJob/part-r-00003").toUri());
        //将TDF结果加载到内存（含某词语的文档总数）
        //注：一般来说语料库中的词语数目是有限的不会很多
        job.addCacheFile(new Path("/sgr/tfidf/output/TDFJob/part-r-00000").toUri());

        FileInputFormat.addInputPath(job,new Path("/sgr/tfidf/output/TFAndDCJob/"));

        Path outPath = new Path("/sgr/tfidf/output/TFIDFJob/");
        //如果存在先删除，保证MR程序正常
        if (fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        boolean result = job.waitForCompletion(true);

        if (result){
            System.out.println("TDFJob done!");
        }
    }
}
