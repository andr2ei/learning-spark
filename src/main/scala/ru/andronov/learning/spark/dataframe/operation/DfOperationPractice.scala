package ru.andronov.learning.spark.dataframe.operation

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.andronov.learning.spark.dataframe.model.ModelCreator
import org.apache.spark.sql.functions._

object DfOperationPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF operation practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val usersDF: DataFrame = ModelCreator.createUserDf(spark)
    val postsDF: DataFrame = ModelCreator.createPostDf(spark)

    usersDF.createOrReplaceTempView("users")
    postsDF.createOrReplaceTempView("posts")

    println("select, alias")
    import spark.implicits._
    val selectedUsersDF = usersDF.select(usersDF.col("firstName").alias("firstNameAlias"), $"lastName")
    selectedUsersDF.show(10, truncate = false)

    println("filter, limit")
    usersDF.filter("firstName == 'John' and age > 20").show(10, truncate = false)
    usersDF.filter(usersDF.col("firstName") === "John" and usersDF.col("age") > 20).show(10, truncate = false)
    usersDF.filter("firstName like '%n'").show(10, truncate = false)
    usersDF.limit(1).show(10, truncate = false)

    println("groupBy, agg")
    val joinedUsersAndPosts = spark.sql(sqlText =
      "SELECT " +
        "u.id, " +
        "u.firstName, " +
        "u.lastName, " +
        "u.age, " +
        "p.id as postId, " +
        "p.text,  " +
        "row_number() over(order by u.age, p.id) as rn, " +
        "avg(age) over(order by u.age, p.id) as avgAge " +
        "FROM users as u " +
        "LEFT JOIN posts as p ON u.id = p.userId")
    joinedUsersAndPosts.groupBy("id", "firstName", "lastName")
      .agg(
        avg("age").alias("avgAge"),
        count("postId").alias("postCount"))
      .show(10, truncate = false)
    postsDF.select("userId").distinct().show(10, truncate = false)

    println("order by")
    postsDF.orderBy("userId", "text").show(10, truncate = false)

    println("with column")
    usersDF.withColumn("gender", lit("m")).show(10, truncate = false)
    usersDF.withColumn("agePlusTen", usersDF("age") + 10).show(10, truncate = false)

    println("with column rename")
    usersDF.withColumn("agePlusTen", usersDF("age") + 10).withColumnRenamed("agePlusTen", "ageTen").show(10, truncate = false)

    println("explain")
    usersDF.explain()

    println("drop")
    usersDF.drop("age").show(10, truncate = false)

    println("describe")
    usersDF.describe().show(10, truncate = false)

    println("head")
    println(usersDF.head())
  }

}
