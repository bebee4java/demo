package com.java.hadoop.mapreduce.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 迭代计算网站排名
 * Created by sgr on 2017/10/19/019.
 */
public class PRJob {
    //PR计算差值
    private static double diff;
    //网页节点总数
    private static long nodeNum;

    public static enum MyCounter{
        my
    }

    public static long getNodeNum() {
        return nodeNum;
    }

    public static double getDiff() {
        return diff;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //计算差值设值
        diff = 0.001;
        //网页节点数目
        nodeNum =4;

        //hdfs安全认证基于用户名匹配
        //客户端想上传文件用户名必须一致
        //1.可以修改客户端的用户名
        //2.不修改客户端用户名，代码中设置HADOOP_USER_NAME变量
        System.setProperty("HADOOP_USER_NAME","sgr");

        Configuration configuration = new Configuration();

        int i = 0;
        while (true){
            i++;
            try{
                configuration.setInt("runCount",i);
                FileSystem fileSystem = FileSystem.get(configuration);
                Job job = Job.getInstance(configuration);
                job.setJobName("pr"+i);
                job.setJarByClass(PRJob.class);
                job.setMapperClass(PRMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(PRReduce.class);
                //格式化输入：以制表符作为分隔，第一个值作为key，后面的值作为value
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                Path inPath = new Path("/sgr/pagerank/input/webpg.txt");

                if (i > 1){
                    inPath = new Path("/sgr/pagerank/output/pr" + (i-1));//取上次分析结果作为输入
                }

                FileInputFormat.addInputPath(job,inPath);

                Path outPath = new Path("/sgr/pagerank/output/pr" + i);
                //如果存在先删除，保证MR程序正常
                if (fileSystem.exists(outPath)){
                    fileSystem.delete(outPath,true);
                }
                FileOutputFormat.setOutputPath(job,outPath);

                boolean result = job.waitForCompletion(true);
                if (result){
                    System.out.println("PRJob done!");

                    long sum = job.getCounters().findCounter(MyCounter.my).getValue();
                    double d = sum * diff / nodeNum;//之前放大了diff倍，要变回去，并且除以node数目
                    System.out.println("===========new avg diff value is "+ d);
                    if (diff > d){
                        break;//差值比diff小 结束迭代
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
