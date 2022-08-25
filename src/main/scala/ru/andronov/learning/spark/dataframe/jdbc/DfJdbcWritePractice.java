package ru.andronov.learning.spark.dataframe.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import java.util.Properties;

public class DfJdbcWritePractice {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DF JDBC practice")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark)
                .select("id", "firstName", "lastName", "address.country");

        userDF.show();

        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        userDF.write()
                .mode(SaveMode.Append)
                .option("createTableColumnTypes",
                        "id INT, firstName VARCHAR(20), lastName VARCHAR(20), country VARCHAR(10)")
                .jdbc("jdbc:postgresql://localhost:5432/learning-spark", "test_2_users", props);
    }
}
