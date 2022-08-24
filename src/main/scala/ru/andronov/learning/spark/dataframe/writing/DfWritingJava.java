package ru.andronov.learning.spark.dataframe.writing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

public class DfWritingJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000")
                .appName("DF writing java")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);
        userDF.write().mode(SaveMode.Overwrite).parquet("/dataframe/writing/second_attempt");
    }
}
