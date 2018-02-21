package com.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by sgr on 2018/2/21/021.
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = sparkContext.textFile("student.csv");
        JavaRDD<Student> studentRDD = javaRDD.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] ss = line.split("\\|");
                Student student = new Student();
                student.setId(ss[0]);
                student.setName(ss[1]);
                student.setAge(Integer.valueOf(ss[2]));
                return student;
            }
        });
        SQLContext sqlContext = new SQLContext(sparkContext);
        //通过反射方式将RDD转换成DataFrame
        Dataset<Row> df = sqlContext.createDataFrame(studentRDD,Student.class);
        df.printSchema();
        //注册成一张临时表student
        df.registerTempTable("student");
        Dataset<Row> ageDf = sqlContext.sql("select * from student where age >= 10");
        JavaRDD<Row> rowRDD = ageDf.toJavaRDD();

        rowRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                //通过反射来生成这个DataFrame的方式如果使用get(index)，注意这个顺序是字典顺序
                /*String id = row.getString(1);
                String name = row.getString(2);
                int age = row.getInt(0);*/

                //建议使用列名来取数据
                String id = row.getAs("id");
                String name = row.getAs("name");
                int age = row.getAs("age");
                Student student = new Student();
                student.setId(id);
                student.setName(name);
                student.setAge(age);

                return student;
            }
        }).foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(student);
            }
        });

        sparkContext.close();
    }
}
