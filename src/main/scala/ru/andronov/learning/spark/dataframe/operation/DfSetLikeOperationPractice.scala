package ru.andronov.learning.spark.dataframe.operation

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.andronov.learning.spark.dataframe.model.ModelCreator

object DfSetLikeOperationPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF operation practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val usersDF: DataFrame = ModelCreator.createUserDf(spark)
    val users2DF: DataFrame = ModelCreator.createUser2Df(spark)

    println("intersect")
    usersDF.intersect(users2DF).show(10, truncate = false)

    println("union")
    usersDF.union(users2DF).show(10, truncate = false)

    println("except")
    usersDF.except(users2DF).show(10, truncate = false)
  }


}
