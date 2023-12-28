package com.pluralsight.flink.module2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{createTypeInformation, ExecutionEnvironment}
import org.slf4j.LoggerFactory

object FilterMovies {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val basePath: String = parameters.get("basePath", ".")
    val path: String = s"$basePath/src/main/resources/ml-latest-small/movies.csv"

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    /*
    // movieId,title,genres
    val lines: DataSet[(Int, String, String)] = env
      .readCsvFile[(Int, String, String)](path, ignoreFirstLine = true)
    // lines.print()

    val movies: DataSet[Movie] = lines.map(line => Movie(line._2, line._3.split('|')))
    // movies.print()

    val filteredMovies: DataSet[Movie] = movies.filter(_.genres.contains("Drama"))
    filteredMovies.print()

    filteredMovies.writeAsText(s"$basePath/filter-output")
     */
    env
      .readCsvFile[(String, String)](path, ignoreFirstLine = true, includedFields = Array(1, 2))
      .map(line => Movie(line._1, line._2.split('|')))
      .filter(_.genres.contains("Drama"))
      .writeAsText(s"$basePath/filter-output")

    env.execute()
  }
}
