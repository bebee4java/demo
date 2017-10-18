package com.java.hadoop.mapreduce.fof;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/18.
 */
public class FoFBondReduce extends Reducer<Text,IntWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        boolean flag = true;
        int sum = 0;
        for (IntWritable i : values){
            if (i.get() == 0){
                //去掉直接好友关系
                flag = false;
                break;
            }
            sum += i.get();//计算亲密度
        }
        if (flag){
            String s = StringUtils.join(key.toString(),"-",sum);
            context.write(new Text(s), NullWritable.get());
        }
    }
}
