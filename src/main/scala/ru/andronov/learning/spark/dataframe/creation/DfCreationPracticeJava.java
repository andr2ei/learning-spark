package ru.andronov.learning.spark.dataframe.creation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

public class DfCreationPracticeJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Df Creation Java")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> usersDF = ModelCreatorJava.createUserDF(spark);

        usersDF.printSchema();
        usersDF.show(10, false);
    }

}
