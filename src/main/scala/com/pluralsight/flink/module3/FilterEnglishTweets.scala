package com.pluralsight.flink.module3

import org.apache.flink.streaming.api.scala.{createTypeInformation, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import java.util.Properties

/**
  * XXX: not working, because twitter api v1.1 is gone.
  */
object FilterEnglishTweets {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    val stream = getClass.getResourceAsStream("/twitter.properties")
    props.load(stream)

    env.addSource(new TwitterSource(props)).print()

    env.execute()
  }
}

case class Tweet(lang: String, text: String) {
  override def toString: String = s"""Tweet{text='$text', lang='$lang'}"""
}
