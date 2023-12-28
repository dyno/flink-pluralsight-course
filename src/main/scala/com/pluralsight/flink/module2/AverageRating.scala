package com.pluralsight.flink.module2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object AverageRating {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val basePath: String = parameters.get("basePath", ".")
    val path: String = s"$basePath/src/main/resources/ml-latest-small/movies.csv"

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // movieId,title,genres
    val movies: DataSet[(Long, String, String)] = env
      .readCsvFile[(Long, String, String)](path, ignoreFirstLine = true)

    // userId,movieId,rating,timestamp
    val ratings: DataSet[(Long, Double)] = env
      .readCsvFile[(Long, Double)](
        "src/main/resources/ml-latest-small/ratings.csv",
        ignoreFirstLine = true,
        includedFields = Array(1, 2)
      )

    val r = movies
      .join(ratings)
      .where(0)
      .equalTo(0)(
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/dataset/transformations/#join-with-join-function
        { (movie, rating) =>
          val name = movie._2
          val genre = movie._3.split('|')(0)
          val score = rating._2

          (name, genre, score)
        }
      )
      .filter(x => !x._2.contains("\"") && !x._2.contains(" "))
      .groupBy(2)
      .reduceGroup(
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/dataset/transformations/#groupreduce-on-dataset-grouped-by-field-position-keys-tuple-datasets-only
        (in: Iterator[(String, String, Double)], out: Collector[(String, Double, Int)]) => {
          val (genre: String, sum: Double, count: Int) =
            in.foldLeft(("", 0.0, 0)) {
              case ((_, sum, count), (_, genre, score)) =>
                (genre, sum + score, count + 1)
            }
          out.collect((genre, sum / count, count))
        }
      )
      .collect()

    r.sortBy(_._2).reverse.foreach(println)
    // env.execute()
  }

}
