package com.java.hadoop.mapreduce.pagerank;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class PRMapper extends Mapper<Text,Text,Text,Text> {

    /**
     * @param key String 当前页面
     * @param value String 当前页面的出链页面
     * @param context String
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        int runCount = context.getConfiguration().getInt("runCount",1);

        String page = key.toString();
        Node node = null;
        if (runCount == 1){
            //第一次迭代 初始化pr值为1.0
            node = Node.formatNode(StringUtils.join("1.0","\t",value.toString()));
        }else {
            node = Node.formatNode(value.toString());
        }
        //将上一次结果输出
        //A 1.0 B C
        context.write(new Text(page), new Text(node.toString()));

        if (node.containsOutlinkNode()) {

            double pr = node.getPr() / node.getOutlinkNodeNames().length;
            for (int i = 0; i < node.getOutlinkNodeNames().length; i++) {
                String outLinkNode = node.getOutlinkNodeNames()[i];
                //A 0.5
                context.write(new Text(outLinkNode), new Text(String.valueOf(pr)));
            }
        }
    }
}
