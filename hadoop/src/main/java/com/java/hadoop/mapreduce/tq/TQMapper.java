package com.java.hadoop.mapreduce.tq;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class TQMapper extends Mapper<LongWritable,Text,WeatherRecord,DoubleWritable> {
    FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key long 行号
     * @param value String 行数据
     * @param context [wr,wd] [温度记录，温度]
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strs = StringUtils.split(line,'\t');

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime((Date) fastDateFormat.parseObject(strs[0]));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        WeatherRecord weatherRecord = new WeatherRecord();
        weatherRecord.setYear(calendar.get(Calendar.YEAR));
        weatherRecord.setMonth(calendar.get(Calendar.MONTH) + 1);
        weatherRecord.setDay(calendar.get(Calendar.DAY_OF_MONTH));
        double wd = Double.parseDouble(strs[1].split("c")[0]);
        weatherRecord.setWd(wd);
        context.write(weatherRecord, new DoubleWritable(wd));
    }
}
