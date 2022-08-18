package ru.andronov.learning.spark.dataframe.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.andronov.learning.spark.dataframe.model.{ModelCreator, Post, User}

object DfSqlPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF sql practice")
      .master("local[*]")
      .getOrCreate()

    val usersDF: DataFrame = ModelCreator.createUserDf(spark)
    val postsDF: DataFrame = ModelCreator.createPostDf(spark)

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

}
