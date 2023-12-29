package com.pluralsight.flink.module2

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{createTypeInformation, DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object Top10Movies {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val basePath: String = parameters.get("basePath", ".")

    val ratingPath: String = s"$basePath/src/main/resources/ml-latest-small/ratings.csv"
    val moviePath: String = s"$basePath/src/main/resources/ml-latest-small/movies.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment

    // userId,movieId,rating,timestamp
    val sorted = env
      .readCsvFile[(Long, Double)](ratingPath, ignoreFirstLine = true, includedFields = Array(1, 2))
      .groupBy(0) // group by movieId
      .reduceGroup(
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/dataset/transformations/#groupreduce-on-dataset-grouped-by-field-position-keys-tuple-datasets-only
        (in: Iterator[(Long, Double)], out: Collector[(Long, Double, Long)]) => {
          val (movieId: Long, sum: Double, count: Int) = in.foldLeft((-1L, 0.0, 0)) {
            case ((_, sum, count), (movieId, score)) => (movieId, sum + score, count + 1)
          }

          if (count > 50) { out.collect((movieId, sum / count, count)) }
        }
      )
      .setParallelism(5)
      .partitionCustom((score: Double, numPartitions: Int) => score.intValue() % numPartitions, 1)
      .sortPartition(1, Order.DESCENDING)
      .mapPartition((in: Iterator[(Long, Double, Long)], out: Collector[(Long, Double, Long)]) => {
        in.take(10).foreach { out.collect }
      })
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
      .mapPartition((in: Iterator[(Long, Double, Long)], out: Collector[(Long, Double, Long)]) => {
        in.take(100).foreach { out.collect }
      })

    // movieId,title,genres
    val movies: DataSet[(Long, String)] = env
      .readCsvFile[(Long, String)](moviePath, ignoreFirstLine = true, includedFields = Array(0, 1))

    val r = movies
      .join(sorted)
      .where(0)
      .equalTo(0) { (movie, sorted) =>
        val (movieId, name) = movie
        val (_, score, count) = sorted
        (movieId, name, score, count)
      }
      .setParallelism(1)
      .sortPartition(2, Order.DESCENDING)
      .collect()

    // XXX: the algorithm here seems wrong, as the result is not what we are expecting.
    LOG.info("Top 10 movies:\n{}", r.zipWithIndex.map({ case (m, idx) => f"${idx + 1}%-2d -> $m" }).mkString("\n"))
  }
}
