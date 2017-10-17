package com.java.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/17/017.
 */
public class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key String word
     * @param values int it<count> 次数
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable i : values){
            sum += i.get();
        }
        context.write(key,new IntWritable(sum));

    }
}
