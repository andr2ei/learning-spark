package ru.andronov.learning.spark.dataframe.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import static org.apache.spark.sql.functions.*;

public class DfSortOperationPracticeJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Df sort operation")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);

        System.out.println("sort");
        userDF.sort(col("age").desc()).show(10, false);


    }
}
