package ru.andronov.learning.spark.dataframe.operation

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.andronov.learning.spark.dataframe.model.ModelCreator

object DfJoinOperationPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF operation practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val usersDF: DataFrame = ModelCreator.createUserDf(spark)
    val postsDF: DataFrame = ModelCreator.createPostDf(spark)

    import spark.implicits._

    println("inner join")
    usersDF.as("u").join(postsDF.as("p"), $"u.id" === $"p.userId", "inner").show(10, truncate = false)

    println("cross join")
    usersDF.crossJoin(postsDF).show(10, truncate = false)

    println("full outer join")
    usersDF.as("u").join(postsDF.as("p"), $"u.id" === $"p.userId", "full_outer").show(10, truncate = false)

    println("left outer")
    postsDF.as("p").join(usersDF.as("u"), $"u.id" === $"p.userId", "left_outer").show(10, truncate = false)
  }


}
