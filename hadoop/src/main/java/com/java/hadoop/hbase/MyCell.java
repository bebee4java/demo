package com.java.hadoop.hbase;

/**
 * Created by sgr on 2017/11/12.
 */
public class MyCell {
    //行
    private String rowKey;
    //列族
    private String cf;
    //列名
    private String cloumnName;
    //值
    private Object cloumnValue;

    public String getRowKey() {
        return rowKey;
    }

    public MyCell(String rowKey, String cf, String cloumnName, Object cloumnValue){
        this.rowKey = rowKey;
        this.cf = cf;
        this.cloumnName = cloumnName;
        this.cloumnValue = cloumnValue;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCf() {
        return cf;
    }

    public void setCf(String cf) {
        this.cf = cf;
    }

    public String getCloumnName() {
        return cloumnName;
    }

    public void setCloumnName(String cloumnName) {
        this.cloumnName = cloumnName;
    }

    public Object getCloumnValue() {
        return cloumnValue;
    }

    public void setCloumnValue(Object cloumnValue) {
        this.cloumnValue = cloumnValue;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "rowKey='" + rowKey + '\'' +
                ", cf='" + cf + '\'' +
                ", cloumnName='" + cloumnName + '\'' +
                ", cloumnValue='" + cloumnValue + '\'' +
                '}';
    }
}
