package ru.andronov.learning.spark.dataframe.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import ru.andronov.learning.spark.dataframe.SparkBase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class DfJoinOperationPracticeJavaTest implements SparkBase {

    @Test
    public void testFullOuterJoin() {
        try(SparkSession spark = spark()) {
            Dataset<Row> userDF = spark.read().json("src/test/resources/join/input_user.jsonl");
            Dataset<Row> postDF = spark.read().json("src/test/resources/join/input_post.jsonl");


            Dataset<Row> joinedDF = DfJoinOperationPracticeJava.fullOuterJoin(postDF, userDF).orderBy("text");

            Dataset<Row> expectedDF = spark.read().json("src/test/resources/join/expected_joined_data.jsonl")
                    .orderBy("text").select("id", "firstName", "lastName", "text");

            assertArrayEquals((Object[]) joinedDF.collect(), (Object[]) expectedDF.collect());
        }
    }
}