package com.java.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class FoFPushMapper extends Mapper<LongWritable,Text,Friend,IntWritable> {

    /**
     * @param key
     * @param value
     * @param context [Friend,hot]
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strs = StringUtils.split(line,'-');
        Friend f1 = new Friend();
        f1.setFriend1(strs[0]);
        f1.setFriend2(strs[1]);
        f1.setHot(Integer.parseInt(strs[2]));
        context.write(f1,new IntWritable(f1.getHot()));

        Friend f2 = new Friend();
        f2.setFriend1(strs[1]);
        f2.setFriend2(strs[0]);
        f2.setHot(Integer.parseInt(strs[2]));
        context.write(f2, new IntWritable(f2.getHot()));

    }
}
