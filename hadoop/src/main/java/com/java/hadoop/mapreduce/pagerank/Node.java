package com.java.hadoop.mapreduce.pagerank;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * 网页节点
 * Created by sgr on 2017/10/19/019.
 */
public class Node {
    //字符串的第一个元素 初始化为1.0
    private double pr = 1.0;
    //字符串后面的节点列表
    private String[] outlinkNodeNames;
    //字段的分隔符
    private static final char fieldSeparator = '\t';

    public String[] getOutlinkNodeNames() {
        return outlinkNodeNames;
    }

    public Node setOutlinkNodeNames(String[] outlinkNodeNames) {
        this.outlinkNodeNames = outlinkNodeNames;
        return this;
    }

    public double getPr() {
        return pr;
    }

    public Node setPr(double pr) {
        this.pr = pr;
        return this;
    }

    public boolean containsOutlinkNode(){
        return this.outlinkNodeNames != null && this.outlinkNodeNames.length > 0;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(pr);
        if (getOutlinkNodeNames() != null){
            sb.append(fieldSeparator).append(
                    StringUtils.join(getOutlinkNodeNames(),fieldSeparator)
            );
        }
        return sb.toString();
    }

    public static Node formatNode(String value) throws IOException {
        String[] strs = StringUtils.split(value,fieldSeparator);
        if (strs.length < 1){
            throw new IOException("data form exception,expected 1 or more parts.data="+value);
        }
        Node node = new Node().setPr(Double.parseDouble(strs[0]));
        if (strs.length > 1){
            node.setOutlinkNodeNames(Arrays.copyOfRange(strs, 1, strs.length));
        }
        return node;
    }
}
