package com.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sgr on 2018/2/22/022.
 */
public class RDD2DataFrameDynamic {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameDynamic");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        JavaRDD<String> javaRDD = sparkContext.textFile("student.csv");
        JavaRDD<Row> rows = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] ss = line.split("\\|");
                return RowFactory.create(ss[0], ss[1], Integer.valueOf(ss[2]));
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = sqlContext.createDataFrame(rows, schema);
        df.registerTempTable("student");
        Dataset<Row> dfAge = sqlContext.sql("select * from student where age >= 10");
        dfAge.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });


    }
}
