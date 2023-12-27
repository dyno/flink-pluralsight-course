package com.pluralsight.flink.module2

import org.apache.flink.table.api._

// import org.apache.flink.api.scala.ExecutionEnvironment

case class Movie(name: String, genres: Seq[String]) {
  override def toString: String = s"""Movie{name='$name', genres=${genres.mkString("[", ",", "]")}}"""
}

object FilterMovies {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val schema = Schema
      .newBuilder()
      .column("_", DataTypes.INT())
      .column("name", DataTypes.STRING())
      .column("genres", DataTypes.STRING())
      .build()

    val path: String = "src/main/resources/ml-latest-small/movies.csv"
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
