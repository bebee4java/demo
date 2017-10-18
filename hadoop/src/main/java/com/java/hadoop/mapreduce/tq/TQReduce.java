package com.java.hadoop.mapreduce.tq;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class TQReduce extends Reducer<WeatherRecord,DoubleWritable,Text,NullWritable> {

    /**
     * @param key 温度记录
     * @param values 温度列表
     * @param context [msg,null]
     */
    @Override
    protected void reduce(WeatherRecord key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        int flag = 0;

        for (DoubleWritable wd : values){
            flag ++;
            if (flag > 2){
                break;
            }
            String msg = StringUtils.join(
                    key.getYear(),"-",
                    key.getMonth(),"-",
                    key.getDay(),"-",
                    wd.get()
            );
            context.write(new Text(msg),NullWritable.get());
        }

    }
}
