package com.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sgr on 2018/2/25/025.
 */
public class JDBCDataSource {
    /**
     * 服务器上执行程序jar包(注意代码里不能写setMaster())
     * 1：
     * ./bin/spark-submit --master spark://node1:7077 --class com.java.spark.sql.JDBCDataSource
     * --driver-class-path ./lib/mysql-connector-java-5.1.39-bin.jar
     * --jars ./lib/mysql-connector-java-5.1.39-bin.jar
     * sparksqljdbcdatasource.jar
     *
     * 2 如果使用standalone cluster模式运行，需要配置spark-env.sh：
     * export SPARK_CLASSPATH=/opt/spark-2.2.0/lib/mysql-connector-java-5.1.39-bin.jar
     * ./bin/spark-submit --master spark://node1:7077 --deploy-mode cluster --class com.java.spark.sql.JDBCDataSource
     * ./sparksqljdbcdatasource.jar
     * 这样会报找不到jar包的错误，因为cluster模式是在从节点里运行的，这里可以将jar包上传到hdfs做修改：
     * hdfs://node1:8020/sparksqljdbcdatasource.jar
     *
     * 3 如果使用yarn模式运行，需要配置conf/spark-defaults.conf:
     * spark.driver.extraClassPath=/opt/spark-2.2.0/lib/mysql-connector-java-5.1.39-bin.jar
     * spark.executor.extraClassPath=/opt/spark-2.2.0/lib/mysql-connector-java-5.1.39-bin.jar
     * 此时兼容前面两种模式，所以配置了spark-defaults.conf前面模式就不需要mysql jar相关的配置
     *
     * 此时后面的jar包需要写本地路径，yarn模式会自动上传jar包到hdfs上：
     * client:
     * ./bin/spark-submit --master yarn-client --class com.java.spark.sql.JDBCDataSource ./sparksqljdbcdatasource.jar
     * cluster:
     * ./bin/spark-submit --master yarn-cluster --class com.java.spark.sql.JDBCDataSource ./sparksqljdbcdatasource.jar
     */
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JDBCDataSource");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Map<String,String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://localhost:3306/test");
        options.put("user","root");
        options.put("password","root");
        options.put("dbtable","student_info");

        Dataset<Row> student_info_df = sqlContext.read().format("jdbc").options(options).load();
        options.put("dbtable","student_score");
        Dataset<Row> student_score_df = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String,Tuple2<Integer,Integer>> result = student_info_df.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>((String) row.getAs("name"), (Integer) row.getAs("age"));
            }
        }).join(student_score_df.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>((String) row.getAs("name"), (Integer) row.getAs("score"));
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2._2 >= 80;
            }
        }));

        JavaRDD<Row> javaRDD = result.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("score",DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> resultDF = sqlContext.createDataFrame(javaRDD,schema);
//        resultDF.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJdbc");

        //将DataFrame的数据保存到jdbc
        resultDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String name = row.getAs("name");
                int age = row.getAs("age");
                int score = row.getAs("score");
                String sql = "insert into good_student_info values('"+name+"',"+age+","+score+")";
                Connection connection = null;
                Statement statement = null;
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
                    statement = connection.createStatement();
                    System.out.println(sql);
                    statement.execute(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    if (statement != null){
                        try {
                            statement.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (connection != null){
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });

        sparkContext.close();
    }
}
