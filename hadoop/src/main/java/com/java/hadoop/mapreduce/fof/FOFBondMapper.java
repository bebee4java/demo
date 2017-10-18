package com.java.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 结合具有二度关系的好友Map
 * Created by sgr on 2017/10/18.
 */
public class FOFBondMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * @param key
     * @param value
     * @param context [x-x,0|1]
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        String[] strs = StringUtils.split(line,' ');
        for (int i=0; i<strs.length; i++){
            //已知好友关系,度为0
            String s1 = FOF.format(strs[0],strs[i]);
            context.write(new Text(s1),new IntWritable(0));
            for (int j=i+1; j<strs.length; j++){
                String s2 = FOF.format(strs[i],strs[j]);
                context.write(new Text(s2),new IntWritable(1));
            }
        }
    }
}
