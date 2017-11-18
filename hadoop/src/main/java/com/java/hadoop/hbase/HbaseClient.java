package com.java.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Triple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by sgr on 2017/11/4.
 */
public class HbaseClient {
    private HBaseAdmin hBaseAdmin;
    private HTable hTable;
    private String tableName;
    public HbaseClient(String tableName){
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean init(){
        Configuration configuration = new Configuration();
        //hbaseadmin通过zk进行访问，不需要将配置文件加载
        configuration.set("hbase.zookeeper.quorum","node1,node2,node3");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            TableName tn = TableName.valueOf(tableName);
            hTable = new HTable(configuration,tn);
        } catch (IOException e) {
            return false;
        }
        return true;
    }


    public boolean createTable(String tableName, String columnFamily){
        try {
            if (hBaseAdmin.tableExists(tableName)){
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
            }
        } catch (IOException e) {
            return false;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //设置列族（建议设置列族个数为：1-3,这边一个就够了）
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        //设置读缓存
        hColumnDescriptor.setBlockCacheEnabled(true);
        //设置加载到内存
        hColumnDescriptor.setInMemory(true);
        //设置最大版本数
        hColumnDescriptor.setMaxVersions(1);
        //添加列族
        hTableDescriptor.addFamily(hColumnDescriptor);
        try {
            hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }

    public void insert(String rowKey, String cf, Map<String,Object> cloumns) throws IOException {
        Put put = new Put(rowKey.getBytes());//rowKey长度有限制（64kb）
        for (Map.Entry<String,Object> entry : cloumns.entrySet()){
            String cloumnName = entry.getKey();
            Object cloumnValue = entry.getValue();
            put.addColumn(cf.getBytes(),cloumnName.getBytes(), toByteArray(cloumnValue));
        }
        hTable.put(put);
    }

    public void insertBatch(List<MyCell> cells) throws IOException {
        String rowKey = null;
        Put put = null;
        boolean first = true;
        List<Put> putList = new ArrayList<Put>();
        for (MyCell myCell : cells){
            String row = myCell.getRowKey();
            String cf = myCell.getCf();
            String columnName = myCell.getCloumnName();
            Object columnValue = myCell.getCloumnValue();
            if (row.equals(rowKey)){
                //同一行
                put.addColumn(cf.getBytes(),columnName.getBytes(),toByteArray(columnValue));
            }else {
                if (!first){
                   putList.add(put);
                }
                //下一行
                put = new Put(row.getBytes());
                put.addColumn(cf.getBytes(),columnName.getBytes(),toByteArray(columnValue));
                if (first){
                    first = false;
                }
            }
            rowKey = row;
        }
        if (put != null){
            putList.add(put);
        }
        hTable.put(putList);
    }

    public List<MyCell> get(String rowKey,String cf, String ... cloumnNames) throws IOException {
        Get get = new Get(rowKey.getBytes());
        //请求服务器只查询你需要的字段
        for (String cloumnName : cloumnNames)
            get.addColumn(cf.getBytes(),cloumnName.getBytes());

        Result result = hTable.get(get);
        List<MyCell> cellList = new ArrayList<MyCell>(result.size());
        for (String cloumnName : cloumnNames){
            Cell cell = result.getColumnLatestCell(cf.getBytes(),cloumnName.getBytes());
            MyCell myCell = new MyCell(rowKey,cf,cloumnName,toObject(CellUtil.cloneValue(cell)));
            cellList.add(myCell);
        }
        return cellList;
    }


    public List<MyCell> scanTable(String startRow, String stopRow,String cf, String... cloumnNames) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner results = hTable.getScanner(scan);
        List<MyCell> cellList = new ArrayList<MyCell>();
        for (Result result : results){
            for (String columnName : cloumnNames){
                Cell cell = result.getColumnLatestCell(cf.getBytes(),columnName.getBytes());
                MyCell myCell = new MyCell(new String(result.getRow()), cf,columnName, new String(CellUtil.cloneValue(cell)));
                cellList.add(myCell);
            }
        }
        return cellList;
    }

    public FilterList getFilterList(boolean mustAll,String cf,String... filterExps){
        FilterList filterList;
        if (mustAll){
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        }else {
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }
        for (String filterExp : filterExps){
            if (filterExp.startsWith("rowKey:")){
                String filter = filterExp.split("rowKey:")[1];
                PrefixFilter prefixFilter = new PrefixFilter(filter.getBytes());
                filterList.addFilter(prefixFilter);
            }else {
                Triple filterTrip = parseOpExp(filterExp);
                if (filterTrip != null){
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter(cf.getBytes(),
                                    (byte[]) filterTrip.getFirst(),
                                    (CompareOp) filterTrip.getSecond(),
                                    (byte[]) filterTrip.getThird());
                    filterList.addFilter(singleColumnValueFilter);
                }
            }
        }
        return filterList;
    }
    private Triple<byte[],CompareOp,byte[]> parseOpExp(String exp){
        if (exp.contains("=")){
            if (exp.contains("!")){
                String[] ss = exp.split("!=");
                return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.NOT_EQUAL,ss[1].getBytes());
            }
            String[] ss = exp.split("=");
            return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.EQUAL,ss[1].getBytes());

        }else if (exp.contains("<")){
            if (exp.contains("=")){
                String[] ss = exp.split("<=");
                return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.LESS_OR_EQUAL,ss[1].getBytes());
            }
            String[] ss = exp.split("<");
            return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.LESS,ss[1].getBytes());
        }else if (exp.contains(">")){
            if (exp.contains("=")){
                String[] ss = exp.split(">=");
                return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.GREATER_OR_EQUAL,ss[1].getBytes());
            }
            String[] ss = exp.split(">");
            return new Triple<byte[],CompareOp,byte[]>(ss[0].getBytes(),CompareOp.GREATER,ss[1].getBytes());
        }else {
            return null;
        }
    }

    /**
     * 通过过滤器查询数据
     * 过滤器的效率并不高，主要的业务过滤放在rowKey中
     * @param filterList 过滤器
     * @param cf 列族
     * @param cloumnNames 列名
     * @return
     * @throws IOException
     */
    public List<MyCell> scanTableWithFilter(FilterList filterList,String cf, String... cloumnNames) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner results = hTable.getScanner(scan);
        List<MyCell> cellList = new ArrayList<MyCell>();
        for (Result result : results){
            for (String columnName : cloumnNames){
                Cell cell = result.getColumnLatestCell(cf.getBytes(),columnName.getBytes());
                MyCell myCell = new MyCell(new String(result.getRow()), cf,columnName, new String(CellUtil.cloneValue(cell)));
                cellList.add(myCell);
            }
        }
        return cellList;
    }


    public void closeTable(){
        if (hTable != null){
            try {
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(){
        if (hBaseAdmin != null){
            try {
                hBaseAdmin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
