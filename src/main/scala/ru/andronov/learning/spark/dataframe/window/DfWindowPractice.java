package ru.andronov.learning.spark.dataframe.window;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import static org.apache.spark.sql.functions.*;

public class DfWindowPractice {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DF window practice")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);

        WindowSpec windowSpec = Window.partitionBy(col("address.country"));
        userDF.withColumn("ageAvg", avg(col("age")).over(windowSpec)).show();
    }

}
