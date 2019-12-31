package com.drake

import java.text.MessageFormat
import java.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema._
import org.junit.Assert._
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object WorkflowConfigValidator {

  class ProcessKeyword(keyword: String) extends AbstractKeyword(keyword) {

    //
    //
    //
    class Validator(inKeyword: String) extends AbstractJsonValidator(inKeyword) {

      val keyword = inKeyword

      override def validate(node: JsonNode, root: JsonNode, at: String): util.Set[ValidationMessage] = {
        //
        val errors = scala.collection.mutable.Set[ValidationMessage]()
        //
        println("*********class level**********")
        println("Entering:: ProcessKeyword.Validator.validate()")
        println("keyword:" + keyword)

        println("*********validate**********")
        println("node:" + node.findValues("name"))
        println("root:" + root.isObject)
        println("at:" + at)
        //
        if (node.findValues("name").size() != node.findValues("name").asScala.map( node => node.asText().toUpperCase() ).toSet.size) {
          errors += buildValidationMessage(CustomErrorMessageType.of("steps[].name", new MessageFormat("{0}: name is not unique")), at)
        }
        //
        if (node.findValues("label").size() != node.findValues("label").asScala.map( node => node.asText().toUpperCase() ).toSet.size) {
          errors += buildValidationMessage(CustomErrorMessageType.of("steps[].name", new MessageFormat("{0}: label is not unique")), at)
        }
        //
        if (node.findValues("model").asScala.filter( t => t.asText().equals("source")).isEmpty) {
          errors += buildValidationMessage(CustomErrorMessageType.of("steps[].model", new MessageFormat("{0}: atleast one source must exist")), at)
        }
        //
        if (node.findValues("model").asScala.filter( t => t.asText().equals("sink")).isEmpty) {
          errors += buildValidationMessage(CustomErrorMessageType.of("steps[].model", new MessageFormat("{0}: atleast one sink must exist")), at)
        }
        //
        val fromMiss = node.findValues("from").asScala diff node.findValues("label").asScala
        if (!fromMiss.isEmpty) {
          fromMiss.foreach( f => {
            errors += buildValidationMessage(CustomErrorMessageType.of("steps[].from", new MessageFormat("{0}: from {1} is not defined as label")), at, f.asText())
          })
        }

        //
        println("Leaving:: ProcessKeyword.Validator.validate()")
        //fail(CustomErrorMessageType.of("tests.example.enumNames", new MessageFormat("{0}: enumName is {1}")), at, value)
        //val l = List[ValidationMessage]()
        //l.map(t => t.getPath).sorted
        errors.asJava
      }

    }
    //  END OF Validator

    //
    //
    //
    //
    def readStringList(node: JsonNode): Array[String] = {
      val result = ArrayBuffer[String]()

      if (!node.isArray()) throw new JsonSchemaException("Keyword enum needs to receive an array")

      node.elements().asScala.foreach(child => result += child.asText())

      result.toArray[String]
    }

    //
    override def newValidator(schemaPath: String, schemaNode: JsonNode, parentSchema: JsonSchema, validationContext: ValidationContext): JsonValidator = {

      println("*********newValidator**********")
      println("schemaPath:" + schemaPath)
      println("schemaNode:" + schemaNode.isArray)
      println("parentSchema:" + parentSchema.toString)
      //You can access the schema node here to read data from your keyword
      if (!schemaNode.isTextual) throw new JsonSchemaException("Keyword process needs to be Text")

      new Validator(getValue())
    }

  }



  def main(args: Array[String]): Unit = {
    //
    println("BEGIN: WorkflowConfigValidator")

    //val objectMapper = new ObjectMapper()
    val objectMapper = new ObjectMapper(new YAMLFactory)

    println("*********step1**********")
    val defSchUri = "https://github.com/networknt/json-schema-validator/tests/schemas/example01#"
    val defAltSchUri = "https://github.com/drake/schemas/member#"

    val metaSchema = JsonMetaSchema
      .builder(defAltSchUri, JsonMetaSchema.getV4())
      // Generated UI uses enumNames to render Labels for enum values
      .addKeyword(new ProcessKeyword("process"))
      .build()

    println("*********step2**********")
    val validatorFactory = JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4)).addMetaSchema(metaSchema).build()
    val schema = validatorFactory.getSchema("{\n" +
      "  \"$schema\":\n" +
      "    \"" + defAltSchUri + "\",\n" +
      "  \"process\": \"mbr\",\n" +
      "  \"accel\": [\"foo\", \"bar\", \"cone\"],\n" +
      "  \"enum\": [\"foo\", \"bar\", \"cone\"],\n" +
      "  \"enumNames\": [\"Foo !\", \"Bar !\", \"Cone !\"]\n" +
      "}")

    println("*********step3**********")
    val messages = schema.validate(objectMapper.readTree(" \"cone\" "))
    println(messages.size())
    //assertEquals(1, messages.size())

    //val message = messages.iterator().next()
    //println(message.getMessage)
    //assertEquals("$: enumName is Cone !", message.getMessage())


    //
    println("END: WorkflowConfigValidator")

    // based on the keyword (or key in the yaml) define validator metaschema
    // once keyword identified in yaml, constructs newValidator
    //   ((at that time it is possible to verify certain keys/types valid or not))
    // Validator constructor will be called once above newValidator is satisfied
    //   ((some value types can be validated as well as cross field value validation))
    // Once above validator is constructed without field value errors
    // validate method will be called
  }

}
