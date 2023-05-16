package com.aslick.strm

import com.aslick.strm.BaseCryptoCodec.{protect, unprotect}
import org.json4s.{JArray, JField, JObject, JString, JValue}
import org.json4s.native.JsonMethods.{parse, render, compact, pretty}

object Model {


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

}
