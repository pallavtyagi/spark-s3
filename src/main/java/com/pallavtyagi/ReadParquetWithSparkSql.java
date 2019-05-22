package com.pallavtyagi;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class ReadParquetWithSparkSql {
    public static void main(String s[]) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = SQLContext.getOrCreate(sc);

        DataFrame df = sqlContext.sql("SELECT * FROM parquet.`/home/pallav/Documents/workspace/code/spark-s3/data/EmpRecord.parquet`");

        List<String> teenagerNames = df.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
    }
}
