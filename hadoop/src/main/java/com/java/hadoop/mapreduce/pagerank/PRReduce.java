package com.java.hadoop.mapreduce.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class PRReduce extends Reducer<Text,Text,Text,Text> {

    /**
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        Node preNode = null;
        for (Text text : values){
            Node node = Node.formatNode(text.toString());
            if (node.containsOutlinkNode()){
                //1.0 A B C
                preNode = node;
            }else {
                //0.5
                sum += node.getPr();
            }
        }

        double pr = ( 0.15 / PRJob.getNodeNum()) + (0.85 * sum);
        System.out.println("==========new pr value is "+ pr);

        double diff = pr - preNode.getPr();
        int d = (int) (diff / PRJob.getDiff());//将数据放大diff倍
        d = Math.abs(d);//取绝对值
        System.out.println("==========new diff value is "+diff);

        context.getCounter(PRJob.MyCounter.my).increment(d);

        preNode.setPr(pr);
        context.write(key, new Text(preNode.toString()));
    }
}
