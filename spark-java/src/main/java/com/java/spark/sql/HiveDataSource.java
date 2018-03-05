package com.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created by sgr on 2018/2/26/026.
 */
public class HiveDataSource {
    /**
     * 该代码本地不能运行，需要打包在服务器上跑：
     * 1.将hive里的hive-site.xml放到spark/conf/目录下
     * 2.启动hive：先启动mysql 然后启动hdfs
     * 3.打包运行
     * 如果你所在的客户端没有把hive-site.xml发送到每台spark的conf目录下，就必须通过--files ./conf/hive-site.xml指定
     *
     * 1.standalone模式：
     * ./bin/spark-submit -master spark://node1:7077 --class com.java.spark.sql.HiveDataSource sparksqlhivedatasource.jar
     * 2.standalone cluster模式:
     * ./bin/spark-submit -master spark://node1:7077 --deploy-mode cluster --class com.java.spark.sql.HiveDataSource
     * --files ./conf/hive-site.xml hdfs://node1:8020/sparksqlhivedatasource.jar
     * 3.yarn-client模式：
     * ./bin/spark-submit --master yarn-client --class com.java.spark.sql.HiveDataSource sparksqlhivedatasource.jar
     * 4.yarn-cluster模式：
     * 如果报：java.lang.ClassNotFoundException: org.datanucleus.api.jdo.JDOPersistenceManagerFactory 通过--jars 指定jar包运行
     * ./bin/spark-submit --master yarn-cluster --class com.java.spark.sql.HiveDataSource
     * --jars ./lib/datanucleus-api-jdo-3.2.1.jar,./lib/datanucleus-core-3.2.2.jar,./lib/datanucleus-rdbms-3.2.1.jar
     * --files ./conf/hive-site.xml sparksqlhivedatasource.jar
     */
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        //判断如果表存在则删除
        spark.sql("DROP TABLE IF EXISTS student_info");
        //判断如果表不存在则创建
        spark.sql("CREATE TABLE IF NOT EXISTS student_info(NAME STRING , AGE INT)");
        //加载数据
        spark.sql("LOAD DATA LOCAL INPATH '/opt/data/student_info.txt' INTO TABLE student_info");

        spark.sql("DROP TABLE IF EXISTS student_score");
        spark.sql("CREATE TABLE IF NOT EXISTS student_score(NAME STRING , SCORE INT)");
        spark.sql("LOAD DATA LOCAL INPATH '/opt/data/student_score.txt' INTO student_score");

        //关联两张表，查询分数大于80的学生
        Dataset<Row> df = spark.sql("select ss.name,si.age,ss.score from student_score ss join student_info si on ss.name = si.name where ss.score >= 80");
        //结果数据存回hive表
        spark.sql("DROP TABLE IF EXISTS good_student_info");
        df.createOrReplaceTempView("good_student_info");

        //读取hive表
        Dataset<Row> result = spark.table("good_student_info");
        Row[] rows = (Row[]) result.collect();
        for (Row row :rows){
            System.out.println(row);
        }
        spark.close();
    }
}
