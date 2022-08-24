package ru.andronov.learning.spark.dataframe.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.array;

public class DfUdfPracticeJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DF UDF Practice")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);

        System.out.println("UDF");
        UDF1<Integer, String> ageToStringUDF = (Integer age) -> age + " years";
        UserDefinedFunction ageToString = udf(ageToStringUDF, DataTypes.StringType);
        userDF.select(col("firstName"), ageToString.apply(col("age")).as("ageStr")).show(10, false);

        System.out.println("explode function");
        userDF.select(col("firstName"), col("lastName"), explode(col("technologies")))
                .show(10, false);
        userDF.select(col("firstName"), explode(col("skills"))).show(10, false);

        System.out.println("array function");
        userDF.select(array(col("firstName"), col("lastName"))).show(10, false);

        System.out.println("array contains function");
        userDF.select(col("firstName"),
                array_contains(col("technologies"), "Scala").as("arrayContains")).show(10, false);

        System.out.println("map values function");
        userDF.select(col("firstName"), map_values(col("skills")).as("skills")).show(10, false);
    }

}
