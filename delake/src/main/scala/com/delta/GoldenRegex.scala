package com.delta

import scala.util.matching.Regex

object GoldenRegex {

  def main4(args: Array[String]): Unit = {

    val cryptoPattern: Regex = "([^\\x00-\\x7F]|[\\w])+".r

    val inputArray = Array(
      "####@@@@@a",
      "1####@@@@@",
      "##00##@@@@@",
      "#a#&0##@@@@",
      "Düsseldorf Köln 北京市 إسرائيل !@#$",
      "Düsseldorf Köln Москва 北京市 إسرائيل !@#$"
    )

    println("Crypto Eligibility")
    inputArray.foreach({input =>
      val eligible = if (cryptoPattern.findAllMatchIn(input).size > 1) true else if (cryptoPattern.findFirstIn(input).getOrElse("").size > 1) true else false
      println(s"$input: $eligible")
    })


  }

  def main3(args: Array[String]): Unit = {

    val date = raw"(\d{4})-(\d{2})-(\d{2})".r

    //val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"
    val dates = "Important dates in history: 2004-10-11 2010"
    val firstDate = date.findFirstIn(dates).getOrElse("No date found.")
    val firstYear = for (m <- date.findFirstMatchIn(dates)) yield m.group(1)
    println(firstDate)
    println(firstYear)

    val sz = date.findAllMatchIn(dates).size
    println("find all matches count: " + sz)
    val allYears = for (m <- date.findAllMatchIn(dates)) yield m.group(1)
    allYears.foreach(println)
    val eligible = if (date.findAllMatchIn(dates).size > 1) true else if (date.findFirstIn(dates).getOrElse("").size > 1) true else false
    println(s"This ($dates) is eligible for encryption: $eligible")


  }


  def main2(args: Array[String]): Unit = {

    val keyValPattern: Regex = "([^\\x00-\\x7F]|[\\w])+".r

    val input: String =
      """Düsseldorf Köln 北京市 إسرائيل !@#$;
        |Düsseldorf Köln Москва 北京市 إسرائيل !@#$""".stripMargin

    for (patternMatch <- keyValPattern.findAllMatchIn(input))
      println(s"key: ${patternMatch}")

  }

  def main1(args: Array[String]): Unit = {

    val keyValPattern: Regex = "([0-9a-zA-Z- ]+): ([0-9a-zA-Z-#()/. ]+)".r

    val input: String =
      """background-color: #A03300;
        |background-image: url(img/header100.png);
        |background-position: top center;
        |background-repeat: repeat-x;
        |background-size: 2160px 108px;
        |margin: 0;
        |height: 108px;
        |width: 100%;""".stripMargin

    for (patternMatch <- keyValPattern.findAllMatchIn(input))
      println(s"key: ${patternMatch.group(1)} value: ${patternMatch.group(2)}")
  }

  def main(args: Array[String]): Unit = {
    main4(args)
  }

}
