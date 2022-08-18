package ru.andronov.learning.spark.dataframe.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.andronov.learning.spark.dataframe.model.{Post, User}

object DfSqlPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF creation practice")
      .master("local[*]")
      .getOrCreate()

    val usersDF: DataFrame = createUserDf(spark)
    val postsDF: DataFrame = createPostDf(spark)

    usersDF.createOrReplaceTempView("users")
    postsDF.createOrReplaceTempView("posts")

    val joinedUsersAndPosts = spark.sql(sqlText =
      "SELECT " +
        "u.firstName, " +
        "u.lastName, " +
        "u.age, " +
        "p.text,  " +
        "row_number() over(order by u.age, p.id) as rn, " +
        "avg(age) over(order by u.age, p.id) as avgAge " +
        "FROM users as u " +
        "LEFT JOIN posts as p ON u.id = p.userId")

    joinedUsersAndPosts.show(10, truncate = false)
  }


  private def createUserDf(spark: SparkSession) = {
    val usersRDD = spark.sparkContext.parallelize(Seq(
      User(1, "John", "Doe", 21),
      User(2, "Jane", "Smith", 25)))

    val usersDF = spark.createDataFrame(usersRDD)
    usersDF
  }

  private def createPostDf(spark: SparkSession) = {
    val postsRDD = spark.sparkContext.parallelize(Seq(
      Post(1, 1, "Text 1 of user 1"),
      Post(2, 1, "Text 2 of user 1"),
      Post(3, 2, "Text 3 of user 2"),
      Post(4, 2, "Text 4 of user 2")))

    val postsDF = spark.createDataFrame(postsRDD)
    postsDF
  }
}
