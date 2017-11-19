package com.java.hadoop.hbase.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/17/017.
 */
public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key long 行号
     * @param value String 行数据
     * @param context [Text,1]
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line,' ');
        for (String word : words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
