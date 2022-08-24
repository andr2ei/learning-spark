package ru.andronov.learning.spark.dataframe;

import org.apache.spark.sql.SparkSession;

public interface SparkBase {

    default SparkSession spark() {
        return SparkSession.builder()
                .appName("Spark Base Test")
                .master("local[*]")
                .getOrCreate();
    }
}
