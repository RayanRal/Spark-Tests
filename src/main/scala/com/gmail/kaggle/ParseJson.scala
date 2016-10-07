package com.gmail.kaggle

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

/**
  * Created by rayanral on 1/15/16.
  */
object ParseJson extends JsonMethods {

  implicit lazy val formats = DefaultFormats

  def parseData(filePath: String) = {
    val rawData = scala.io.Source.fromFile(filePath).getLines.mkString
    parse(rawData).extract[List[Dish]]
  }

}
