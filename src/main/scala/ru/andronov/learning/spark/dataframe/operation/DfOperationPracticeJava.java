package ru.andronov.learning.spark.dataframe.operation;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DfOperationPracticeJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DF operation practice")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> userDF = ModelCreatorJava.createUserDF(spark);

        System.out.println("select limit");
        userDF.select(userDF.col("firstName").alias("firstNameAlias")).show(10, false);
        userDF.limit(1).show(10, false);

        System.out.println("multiple columns selection");
        List<Column> columns = new ArrayList<Column>();
        columns.add(userDF.col("firstName"));
        columns.add(userDF.col("lastName"));
        userDF.select(columns.toArray(new Column[0])).show(10, false);

        System.out.println("with column update");
        userDF.withColumn("age", col("age").plus(lit(10))).show(10, false);

        System.out.println("with column insert");
        userDF.withColumn("ageMultiplied", col("age").multiply(2)).show(10, false);

        System.out.println("renaming nested columns");
        StructType schema1 = new StructType()
                .add("countryNew", "string")
                .add("streetNew", "string")
                .add("houseNumberNew", "integer");
        userDF.select(col("address").cast(schema1), col("firstName"), col("lastName"), col("age")).printSchema();

        System.out.println("filter and where");
        userDF.filter(col("age").leq(18)).show(10, false);
        userDF.where("age <= 18").show(10, false);
        userDF.where(col("age").leq(18)
                .and(col("firstName").equalTo("John")))
                .show(10, false);

        System.out.println("when otherwise");
        userDF.withColumn("generation",
                when(col("age").gt(23), "old")
                        .otherwise("young")).show(10, false);
        userDF.withColumn("generation",
                expr("case when age >= 23 then 'old' else 'young' end")).show(10, false);

        System.out.println("distinct and drop duplicates");
        userDF.select("firstName", "lastName").distinct().show(10, false);
        userDF.dropDuplicates("firstName", "age").show(10, false);

        System.out.println("group by and agg");
        userDF.groupBy("address.country", "address.street")
                .agg(sum("age").as("ageSum"), avg("age").as("ageAvg"))
                .show(10, false);

        System.out.println("map");
        Dataset<Row> tuple2Dataset = userDF.map((MapFunction<Row, Tuple2<String, Integer>>) row ->
                        new Tuple2<>(row.getString(1) + " " + row.getString(2), row.getInt(3)),
                Encoders.tuple(Encoders.STRING(), Encoders.INT())).toDF("fullName", "age");
        tuple2Dataset.show(10, false);

        System.out.println("map partitions");
        userDF.mapPartitions((MapPartitionsFunction<Row, Integer>) iterator -> {
            List<Integer> ages = new ArrayList<>();
            while (iterator.hasNext()) {
                ages.add(iterator.next().getInt(3));
            }
            return ages.iterator();
        }, Encoders.INT()).toDF("age").show(10, false);

        System.out.println("foreach partition");
        userDF.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        });

        System.out.println("sampling");
        userDF.sample(true, 0.5, 123).show(10, false);

        System.out.println("repartition by range");
        userDF.repartitionByRange(4, col("age")).show(10, false);


    }

}
