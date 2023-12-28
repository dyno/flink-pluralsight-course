package com.pluralsight.flink.module2

case class AppArgs(
  basePath: String = "."
)

object AppArgs {

  def parse(args: Array[String]): Option[AppArgs] = {
    val parser = new scopt.OptionParser[AppArgs]("Flink Quickstart App") {
      opt[String]("basepath")
        .optional()
        .action((x, c) => c.copy(basePath = x))
        .text("Environment [prod-test, prod-prod].")
    }

    parser.parse(args, AppArgs())
  }
}
