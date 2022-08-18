package ru.andronov.learning.spark.dataframe.model

import org.apache.spark.sql.SparkSession

import java.sql.Date

object ModelCreator {

  def createUserDf(spark: SparkSession) = {
    val usersRDD = spark.sparkContext.parallelize(Seq(
      User(1, "John", "Doe", 21, Date.valueOf("2000-10-23")),
      User(2, "Jane", "Smith", 25, Date.valueOf("2000-11-05"))))

    val usersDF = spark.createDataFrame(usersRDD)
    usersDF
  }

  def createUser2Df(spark: SparkSession) = {
    val usersRDD = spark.sparkContext.parallelize(Seq(
      User(1, "John", "Doe", 21, Date.valueOf("2000-10-23")),
      User(3, "Jane", "Smith", 25, Date.valueOf("2000-11-05")) ))

    val usersDF = spark.createDataFrame(usersRDD)
    usersDF
  }

  def createPostDf(spark: SparkSession) = {
    val postsRDD = spark.sparkContext.parallelize(Seq(
      Post(1, 1, "Text 1 of user 1"),
      Post(2, 1, "Text 2 of user 1"),
      Post(3, 2, "Text 3 of user 2"),
      Post(4, 2, "Text 4 of user 2"),
      Post(5, 0, "Text 5 of anonymous user")))

    val postsDF = spark.createDataFrame(postsRDD)
    postsDF
  }

}
