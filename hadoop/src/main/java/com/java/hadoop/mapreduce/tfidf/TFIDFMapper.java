package com.java.hadoop.mapreduce.tfidf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TFIDFMapper extends Mapper<LongWritable,Text,Text,Text> {

    //文档总数
    private static Map<String,Integer> dMap = null;
    //包含某个词语的文档总数
    private static Map<String,Integer> dfMap = null;

    /**
     * Called once at the beginning of the task.
     * 在调用map方法之前执行
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("=======setup=======");
        if (dMap == null || dMap.size() == 0 || dfMap == null || dfMap.size() == 0){
            URI[] files =  context.getCacheFiles();
            for (URI f : files){
                if (f.getPath().endsWith("part-r-00003")){
                    //记录文档总数的文件
                    Path path = new Path(f.getPath());
                    BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                    String line = br.readLine();
                    if (line.contains("count")){
                        String[] strs = line.split("\t");
                        dfMap = new HashMap<String, Integer>();
                        dfMap.put(strs[0],Integer.valueOf(strs[1].trim()));
                    }
                    br.close();//关闭流
                }else if (f.getPath().endsWith("part-r-00000")){
                    //记录df记录的文件
                    dfMap = new HashMap<String, Integer>();
                    Path path = new Path(f.getPath());
                    BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                    String line = null;
                    while ((line = br.readLine()) != null){
                        String[] strs = line.split("\t");
                        dfMap.put(strs[0],Integer.valueOf(strs[1]));
                    }
                    br.close();//关闭流
                }else {
                    System.out.println("error input file:" + f.getPath());
                }
            }
        }
    }

    /**
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        if (!fileSplit.getPath().getName().endsWith("part-r-00003")){
            //word_id   count
            String[] strs = value.toString().trim().split("\t");
            if (strs.length == 2){
                int tf = Integer.valueOf(strs[1].trim());//词语在文档出现的次数
                String[] ss = strs[0].split("_");
                if (ss.length == 2){
                    String word = ss[0];
                    String id = ss[1];

                    double tdidf = tf * Math.log(dMap.get("count") / dfMap.get(word));
                    NumberFormat numberFormat = NumberFormat.getNumberInstance();
                    numberFormat.setMaximumFractionDigits(5);//格式化数值
                    context.write(new Text(id), new Text(StringUtils.join(word,":",numberFormat.format(tdidf))));
                }
            }
        }

    }
}
