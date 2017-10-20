package com.java.hadoop.mapreduce.tfidf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TFDCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * 用分词器拆分数据
     * @param key long 行号
     * @param value String 行数据
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strs = line.trim().split("\t");
        if (strs.length == 2){
            String id = strs[0].trim();
            String content = strs[1].trim();
            //分词
            StringReader sr = new StringReader(content);
            IKSegmenter ikSegmenter = new IKSegmenter(sr,true);
            Lexeme word = null;
            while ((word = ikSegmenter.next()) != null){
                String s = StringUtils.join(word.getLexemeText(),"_",id);
                context.write(new Text(s), new IntWritable(1));//输出每个单词_id
            }
            context.write(new Text("count"),new IntWritable(1));
        }else {
            System.out.println("error text:"+line);
        }
    }
}
