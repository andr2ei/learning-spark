package ru.andronov.learning.spark.dataframe.creation

import org.apache.spark.sql.SparkSession
import ru.andronov.learning.spark.dataframe.model.User

object DfCreationPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF creation practice")
      .master("local[*]")
      .getOrCreate()

    val usersRDD = spark.sparkContext.parallelize(Seq(
      User(1, "John", "Doe", 21),
      User(2, "Jane", "Smith", 25)))

    val usersDF = spark.createDataFrame(usersRDD)

    usersDF.show(10, truncate = false)
  }

}
