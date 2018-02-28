package com.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/22/022.
 */
public class JsonDataSource {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JsonDataSource");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> df = sqlContext.read().json("student.json");
        df.registerTempTable("student_score");
        Dataset<Row> goodStudents = sqlContext.sql("select name,score from student_score where score >= 80");
        List<String> goodStudentNames = goodStudents.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("name");
            }
        }).collect();

        List<String> studentInfos = Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":22}",
                "{\"name\":\"lisi\",\"age\":20}",
                "{\"name\":\"wangwu\",\"age\":19}",
                "{\"name\":\"lilei\",\"age\":24}"
        );
        JavaRDD<String> javaRDD = sparkContext.parallelize(studentInfos);
        Dataset<Row> studentInfosDF = sqlContext.read().json(javaRDD);
        studentInfosDF.registerTempTable("student_info");
        String sql = "select name,age from student_info where name in (";
        for (int i=0; i<goodStudentNames.size(); i++){
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1){
                sql += ",";
            }else {
                sql += ")";
            }
        }
        System.out.println(sql);
        Dataset<Row> goodStudentInfos = sqlContext.sql(sql);
        JavaPairRDD<String, Tuple2<Long,Long>> goodStudentsRDD = goodStudentInfos.toJavaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>((String) row.getAs("name"), (Long) row.getAs("age"));
            }
        }).join(goodStudents.toJavaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>((String) row.getAs("name"), (Long) row.getAs("score"));
            }
        }));

        JavaRDD<Row> rows = goodStudentsRDD.map(new Function<Tuple2<String,Tuple2<Long,Long>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Long, Long>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        });
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age",DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("score",DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> result = sqlContext.createDataFrame(rows,schema);
        result.write().format("json").mode(SaveMode.Overwrite).save("goodStudentsJson");
        sparkContext.close();
    }
}
