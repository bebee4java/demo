package com.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sgr on 2018/2/21/021.
 */
public class SpecifyFormatLoadSave {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("SpecifyFormatLoadSave")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("json").load("person.json");
        df.select("name").write().format("parquet").save("person.parquet");
        sparkSession.close();
    }
}
