package ru.andronov.learning.spark.dataframe.creation

import org.apache.spark.sql.SparkSession
import ru.andronov.learning.spark.dataframe.model.{ModelCreator, User}

object DfCreationPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF creation practice")
      .master("local[*]")
      .getOrCreate()

    val usersDF = ModelCreator.createUserDf(spark)

    usersDF.show(10, truncate = false)
    usersDF.printSchema()
  }

}
