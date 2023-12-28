package com.pluralsight.flink.module2

import org.apache.flink.table.api._

/**
  * FilterMovies with TableApi
  *
  * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/tableapi/
  */
object FilterMoviesTableApi {

  def main(args: Array[String]): Unit = {
    val appArgs: AppArgs = AppArgs.parse(args).getOrElse(throw new IllegalArgumentException)
    val basePath: String = appArgs.basePath
    val path: String = s"$basePath/src/main/resources/ml-latest-small/movies.csv"

    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    // movieId,title,genres
    val schema = Schema
      .newBuilder()
      .column("_", DataTypes.INT())
      .column("name", DataTypes.STRING())
      .column("genres", DataTypes.STRING())
      .build()


    val tableDescriptor: TableDescriptor = TableDescriptor
      .forConnector("filesystem")
      .schema(schema) // schema of the table
      .option("path", path)
      .option("csv.ignore-parse-errors", "true")
      .format(FormatDescriptor.forFormat("csv").build())
      .build()

    tableEnv.createTemporaryTable("movies", tableDescriptor)

    val table1 = tableEnv.from("movies").where($"genres".like("%Drama%"))
    table1.execute().print()
  }

}
