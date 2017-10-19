package com.java.hadoop.mapreduce.fof;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class FOFPushReduce extends Reducer<Friend,IntWritable,Text,NullWritable> {

    /**
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Friend key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable i : values){
            String s = StringUtils.join(key.getFriend1(),"-",key.getFriend2(),":",i.get());
            context.write(new Text(s),NullWritable.get());
        }

    }
}
