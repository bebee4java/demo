package com.java.hadoop.mapreduce.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TDFMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        if (!fileSplit.getPath().getName().contains("part-r-00003")){
            //过滤掉最后一个分区的结果：不需要文档总数
            String line = value.toString();
            //word_id   TF
            String[] strs = line.split("\t");
            String[] ss = strs[0].split("_");
            if (ss.length == 2){
                String word = ss[0];
                context.write(new Text(word), new IntWritable(1));
            }else {
                System.out.println("error data:"+strs[0]);
            }
        }

    }
}
