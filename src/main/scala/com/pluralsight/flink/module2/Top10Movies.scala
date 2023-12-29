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
    val topN: Int = 10

    val env = ExecutionEnvironment.getExecutionEnvironment
    LOG.info(s"env.getParallelism=${env.getParallelism}")

    // userId,movieId,rating,timestamp
    val sorted = env
      .readCsvFile[(Long, Double)](ratingPath, ignoreFirstLine = true, includedFields = Array(1, 2))
      .groupBy(0) // DataSet of (movieId, score) and group by movieId
      .reduceGroup(
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/dataset/transformations/#groupreduce-on-dataset-grouped-by-field-position-keys-tuple-datasets-only
        (in: Iterator[(Long, Double)], out: Collector[(Long, Double, Long)]) => {
          val (movieId: Long, sum: Double, count: Int) = in.foldLeft((-1L, 0.0, 0)) {
            case ((_, sum, count), (movieId, score)) => (movieId, sum + score, count + 1)
          }

          if (count > 50) { out.collect((movieId, sum / count, count)) }
        }
      )
      .partitionCustom((movieId: Long, numPartitions: Int) => movieId.toInt % numPartitions, 0)
      .sortPartition(1, Order.DESCENDING) // order by average rating
      .mapPartition((in: Iterator[(Long, Double, Long)], out: Collector[(Long, Double, Long)]) => {
        in.take(topN).foreach { out.collect }
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
      .collect()
      .sortBy(_._3)
      .reverse
      .take(topN)

    LOG.info(s"Top $topN movies:\n{}", r.zipWithIndex.map({ case (m, idx) => f"${idx + 1}%-2d -> $m" }).mkString("\n"))
  }
}
