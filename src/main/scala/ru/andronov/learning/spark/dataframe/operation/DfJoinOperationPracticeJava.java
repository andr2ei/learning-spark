package ru.andronov.learning.spark.dataframe.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import static org.apache.spark.sql.functions.col;

public class DfJoinOperationPracticeJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("df join operation")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> postDf = ModelCreatorJava.createPostDf(spark);
        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);

        Dataset<Row> joinedDF = fullOuterJoin(postDf, userDF);
        joinedDF.show(10, false);
    }

    public static Dataset<Row> fullOuterJoin(Dataset<Row> postDf, Dataset<Row> userDF) {
       return userDF.as("u").join(postDf.as("p"),
                col("u.id").equalTo(col("p.userId")), "full_outer")
                .select("u.id", "u.firstName", "u.lastName", "p.text");
    }


}
