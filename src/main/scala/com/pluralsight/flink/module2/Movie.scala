package com.pluralsight.flink.module2

case class Movie(name: String, genres: Seq[String]) {
  override def toString: String = s"""Movie{name='$name', genres=${genres.mkString("[", ",", "]")}}"""
}
