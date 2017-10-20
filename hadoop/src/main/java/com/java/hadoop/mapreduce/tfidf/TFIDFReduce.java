package com.java.hadoop.mapreduce.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TFIDFReduce extends Reducer<Text,Text,Text,Text> {
    /**
     * @param key 文档id
     * @param values
     * @param context [id,String]
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text text : values){
            sb.append(text).append("\t");
        }
        context.write(key, new Text(sb.toString()));

    }
}
