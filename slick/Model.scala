package com.aslick.strm

import com.aslick.strm.BaseCryptoCodec.{protect, unprotect}
import org.json4s.{JArray, JField, JObject, JString, JValue}
import org.json4s.native.JsonMethods.{compact, parse, pretty, render}
import org.json4s.Xml.{toJson, toXml}
import org.slf4j.LoggerFactory

import scala.xml.XML

object Model {

  val logger = LoggerFactory.getLogger(getClass)

  /**
    * Define scala function
    */
  def xmlToJson(input: String, startAttrs: Map[String, String]): JValue = {

    //
    var jsonValue: JValue = null

    // get config attributes
    val removeProlog = startAttrs.getOrElse("xmlRemoveProlog", "false").toBoolean
    val removeNamespaces = startAttrs.getOrElse("xmlRemoveNamespaces", "").split(",")
    val adjustNamespaces = startAttrs.getOrElse("xmlAdjustNamespaces", "").split(",")
    val elemsToArray = startAttrs.getOrElse("xmlArrayTransformTags", "").split(",")
    val applyCamelizedKeys = startAttrs.getOrElse("xmlApplyCamelizedKeys", "false").toBoolean


    /**
      * Identify XML Elements to remove
      *
      * @param element
      * @param beginTag
      * @param startElem
      * @param endElem
      * @param begInd
      * @param endInd
      * @return
      */
    def recursiveElement(element: String, beginTag: Boolean = false, startElem: String = "<?", endElem: String = "?>", begInd: Int = -1, endInd: Int = -1): String = {
      //
      var ind = element.indexOf(if (beginTag) startElem else endElem, begInd)

      //
      if (ind != -1)
        if (beginTag)
          recursiveElement(element, false, startElem, endElem, ind)
        else
          element.substring(ind+endElem.length).trim
      else
        element.trim
    }

    // remove comments
    var remCommInputVar = input.trim
    var recComInd = remCommInputVar.indexOf("<!--")
    while (recComInd != -1) {
      remCommInputVar = recursiveElement(remCommInputVar, true, "<!--", "-->")
      recComInd = remCommInputVar.indexOf("<!--")
    }
    val remCommInput = remCommInputVar
    logger.debug("remCommInput: " + remCommInput)

    // remove comments
    val remProInput = if (removeProlog) recursiveElement(remCommInput, true) else remCommInput
    logger.debug("remProInput: " + remProInput)

    // remove namespaces
    val remInput = removeNamespaces.foldLeft(remProInput) {(acc, ns) => (acc.replaceAll(ns+":","")) }
    logger.debug("remInput: " + remInput)

    // adjust namespaces (keeps namespace but removes colon)
    val adjInput = adjustNamespaces.foldLeft(remInput) {(acc, ns) => (acc.replaceAll(ns+":", ns)) }
    logger.debug("adjInput: " + adjInput)


    /**
      * Scan thru the inut for array able elements to provide array flavor
      *
      * @param mainInput
      * @param elem
      * @return
      */
    def enhanceToArrayTags(mainInput: String, elem: String): String = {

      //
      val (tagChar, runCheck) =
        if (mainInput.indexOf("<"+elem+">") != -1 || mainInput.indexOf("<"+elem+" ") != -1)
          if (mainInput.indexOf("<"+elem+">") != -1)
            (">", true)
          else
            (" ", true)
        else
          (">", false)

      //
      val beginTag = "<"+elem+tagChar
      logger.debug("beginTag: " + beginTag)


      def recursiveArrayTags(element: String, word: String, wordTag: String, elemCount: Int = 0, mainBegin: Int = -1, mainEnd: Int = -1, chunkBegin: Int = -1, chunkEnd: Int = -1): String = {
        //
        val dfltElem = if (elemCount == 1) "<" + word + "></" + word + ">" else ""

        //
        val lastli = element.indexOf("</" + word + ">", chunkBegin)
        val lastbi = element.indexOf(wordTag, lastli)
        val isFilledWithEmpty =
          if (lastbi != -1 && element.substring((lastli + word.length + 3), lastbi).trim.length == 0)
            true
          else
            false
        logger.debug("next close index: " + lastli + " next begin index: " + lastbi + " is Empty: " + isFilledWithEmpty)

        //
        val retElement =
          if ( (lastbi == -1) || ((lastbi > (lastli + word.length + 3)) && !isFilledWithEmpty) ) {

            //
            val chunkElement = element.substring(0, mainBegin) +
              "<" + word + "Array>" +
              element.substring(mainBegin, lastli + word.length + 3) +
              dfltElem +
              "</" + word + "Array>" +
              element.substring(lastli + word.length + 3, element.length)
            logger.debug("chunk element: " + chunkElement)

            //
            val mainli = lastli + (2*word.length) + (2*"Array".length) + (2*2) + 1 + dfltElem.length
            val mainbi = chunkElement.indexOf(wordTag, mainli)

            //
            if (mainbi != -1)
              recursiveArrayTags(chunkElement, word, wordTag, 1, mainbi, mainli, mainbi, lastli)
            else
              chunkElement


          } else {

            //
            recursiveArrayTags(element, word, wordTag, (elemCount+1), mainBegin, mainEnd, lastbi, lastli)
          }

        //
        retElement
      }



      // call inner func
      val retMainElem =
        if (runCheck) {
          //
          val mainbi = mainInput.indexOf(beginTag, -1)
          logger.debug("main begin index: " + mainbi)
          recursiveArrayTags(mainInput, elem, beginTag, 1, mainbi, -1, mainbi, -1)

        } else {
          //
          mainInput
        }

      //
      retMainElem
    }

    //
    if (adjInput.length > 0) {
      //
      val replArrayInput = elemsToArray.foldLeft(adjInput) { (acc, s) =>
        enhanceToArrayTags(acc, s)
      }

      //
      val jsonRaw = toJson(XML.loadString("<tojson>"+ replArrayInput +"</tojson>").child)
      jsonValue = if (applyCamelizedKeys) jsonRaw.camelizeKeys else jsonRaw

    }

    //
    jsonValue
  }



  /**
    * A class to represent json value to allow replace transformation with nested search
    * @param underlying
    */
  implicit class JValueOps(underlying:JValue) {

    object ArrayIndex {
      val R = """^([^\[]+)\[(\d+)\]""".r
      def unapply(str: String): Option[(String, Int)] = str match {
        case R(name, index) => Option(name, index.toInt)
        case _ => None
      }
    }

    object ArrayAll {
      val R = """^([^\[]+)\[\]""".r
      def unapply(str: String): Option[String] = str match {
        case R(name) => Option(name)
        case _ => None
      }
    }

    /**
      * Returns a value from protect or unprotect function
      * @param inValue
      * @param scheme
      * @param cryptoType
      * @return
      */
    def cryptoValue(inValue: JValue, scheme: String, cryptoType: String): JValue = {
      inValue match {
        case JString(s) => JString(if (cryptoType.equalsIgnoreCase("UNPROTECT")) unprotect(s, scheme) else protect(s, scheme))
        case inValue => inValue
      }
    }


    //def jsonReplace(list1: List[String], scheme1: String, cryptoType: String): JValue = {
    /**
      * Returns a replaced value with nested path to search a field
      * @param fieldnscheme
      * @param cryptoType
      * @return
      */
    def jsonReplace(fieldnscheme: String, cryptoType: String): JValue = {

      val jsplitstmp = if (fieldnscheme.indexOf("::") > 0) fieldnscheme.split("::") else  fieldnscheme.split("\\.")
      val jsplits = (jsplitstmp.dropRight(1) :+ jsplitstmp.last.split(":")(0)).map(t => t.trim)
      val scheme = jsplitstmp.last.split(":")(1)
      val list: List[String] = jsplits.toList

      /**
        * Returns the replaced value
        * @param replist
        * @param in
        * @return
        */
      def rep(replist: List[String], in: JValue): JValue = {

        (replist, in) match {

          // eg "foo[0]"
          case (ArrayIndex(name, index) :: Nil, JObject(fields)) => JObject(
            fields.map {
              case JField(`name`, JArray(array)) if array.length > index => JField(name, JArray(array.updated(index, cryptoValue(array(index), scheme, cryptoType))))
              case field => field
            }
          )

          // eg "foo[0]" "bar"
          case (ArrayIndex(name, index) :: xs, JObject(fields)) => JObject(
            fields.map {
              case JField(`name`, JArray(array)) if array.length > index => JField(name, JArray(array.updated(index, rep(xs, array(index)))))
              case field => field
            }
          )

          // eg "foo[]"
          case (ArrayAll(name) :: Nil, JObject(fields)) => JObject(
            fields.map {
              case JField(`name`, JArray(array)) => JField(name, JArray(array.map(t => cryptoValue(t, scheme, cryptoType))))
              case field => field
            }
          )

          // eg "foo[]" "bar"
          case (ArrayAll(name) :: xs, JObject(fields)) => JObject(
            fields.map {
              case JField(`name`, JArray(array)) => JField(name, JArray(array.map( elem => rep(xs, elem))))
              case field => field
            }
          )

          // eg "foo"
          case (x :: Nil, JObject(fields)) => JObject(
            fields.map {
              case JField(`x`, value) ⇒ JField(x, cryptoValue(value, scheme, cryptoType))
              case field ⇒ field
            }
          )

          // eg "foo" "bar"
          case (x :: xs, JObject(fields)) => JObject(
            fields.map {
              case JField(`x`, value) ⇒ JField(x, rep(xs, value))
              case field ⇒ field
            }
          )

          case _ => in

        }

      }

      rep(list, underlying)
    }


    /**
      * Returns a protect value with json replace value with nested path to search a field
      * @param fieldnscheme
      * @return
      */
    def jsonReplaceProtect(fieldnscheme: String): JValue = {
      jsonReplace(fieldnscheme, "PROTECT")
    }


    /**
      * Returns a unprotect value with json replace value with nested path to search a field
      * @param fieldnscheme
      * @return
      */
    def jsonReplaceUnprotect(fieldnscheme: String): JValue = {
      jsonReplace(fieldnscheme, "UNPROTECT")
    }


  }


  /**
    * Returns compact render string as single line
    * @param jstr
    * @return
    */
  def jsonCompactRender(jstr: String): String = {
    compact(render(parse(jstr)))
  }

  /**
    * Returns a protect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonProtect(jstr: String, fieldnscheme: String): String = {
    compact(render(parse(jstr).jsonReplaceProtect(fieldnscheme)))
  }

  /**
    * Returns a protect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonCompactProtect(jstr: String, fieldnscheme: String): String = {
    compact(render(parse(jstr).jsonReplaceProtect(fieldnscheme)))
  }

  /**
    * Returns a protect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonPrettyProtect(jstr: String, fieldnscheme: String): String = {
    pretty(render(parse(jstr).jsonReplaceProtect(fieldnscheme)))
  }

  /**
    * Returns a unprotect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonUnprotect(jstr: String, fieldnscheme: String): String = {
    compact(render(parse(jstr).jsonReplaceUnprotect(fieldnscheme)))
  }

  /**
    * Returns a unprotect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonCompactUnprotect(jstr: String, fieldnscheme: String): String = {
    compact(render(parse(jstr).jsonReplaceUnprotect(fieldnscheme)))
  }

  /**
    * Returns a unprotect value with json replace value with nested path to search a field
    *
    * @param jstr
    * @param fieldnscheme
    * @return
    */
  def jsonPrettyCompactUnprotect(jstr: String, fieldnscheme: String): String = {
    pretty(render(parse(jstr).jsonReplaceUnprotect(fieldnscheme)))
  }

  /**
    * Returns a map of string,string from flattened string
    *
    * @param attrs
    * @return
    */
  def toAttributes(attrs: String): Map[String, String] = {
    attrs.split("::").map(_.split("=")).map(arr => arr(0) -> arr(1)).toMap
  }

  /**
    * Returns a protect value for xml string as json replace value with nested path to search a field
    *
    * @param xstr
    * @param fieldnscheme
    * @return
    */
  def xmlProtect(xstr: String, fieldnscheme: String): String = {
    toXml(toJson(XML.loadString(xstr)).jsonReplaceProtect(fieldnscheme)).toString()
  }

  /**
    * Returns a protect value for xml string as json replace value with nested path to search a field
    *
    * @param xstr
    * @param fieldnscheme
    * @param attrs
    * @return
    */
  def xmlAttrProtect(xstr: String, fieldnscheme: String, attrs: String): String = {
    toXml(xmlToJson(xstr, toAttributes(attrs)).jsonReplaceProtect(fieldnscheme)).toString()
  }

  /**
    * Returns a unprotect value for xml string as json replace value with nested path to search a field
    *
    * @param xstr
    * @param fieldnscheme
    * @return
    */
  def xmlUnprotect(xstr: String, fieldnscheme: String): String = {
    toXml(toJson(XML.loadString(xstr)).jsonReplaceUnprotect(fieldnscheme)).toString()
  }

  /**
    * Returns a unprotect value for xml string as json replace value with nested path to search a field
    *
    * @param xstr
    * @param fieldnscheme
    * @param attrs
    * @return
    */
  def xmlAttrUnprotect(xstr: String, fieldnscheme: String, attrs: String): String = {
    toXml(xmlToJson(xstr, toAttributes(attrs)).jsonReplaceUnprotect(fieldnscheme)).toString()
  }


  def main(args: Array[String]): Unit = {
    val xstr ="<messages><profile><id>1</id><score>100</score><avatar>path.jpg</avatar></profile><profile><id>1</id><score>100</score><avatar>path.jpg</avatar></profile></messages>"
    val scheme = "messages.profile[1].avatar:FIRST_NAME"
    val tj = toJson(XML.loadString(xstr))
    println(compact(render(tj)))
    val prottj = tj.jsonReplaceProtect(scheme)
    println(prottj)
    val xm10 = xmlProtect(xstr, scheme)
    val xm11 = "ddd"
    println(xm11)
  }

}
