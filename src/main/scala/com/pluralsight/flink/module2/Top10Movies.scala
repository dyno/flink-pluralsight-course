package com.pluralsight.flink.module2

import org.apache.flink.api.scala.{createTypeInformation, ExecutionEnvironment}

object Top10Movies {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // userId,movieId,rating,timestamp
    val ds =
      env.readCsvFile[(Long, Long, Double, Long)]("src/main/resources/ml-latest-small/ratings.csv", ignoreFirstLine = true)
        .groupBy(_._2) // group by movieId
        .reduceGroup(iter => iter.toSeq.sortBy(_._3).reverse.take(2))
        .setParallelism(5)

    ds.print()
  }

}
