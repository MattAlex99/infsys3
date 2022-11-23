import com.google.gson.Gson
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.dsl.expressions.windowSpec
import org.apache.spark.sql.functions.{count, countDistinct}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions._

import scala.collection.LinearSeq
//import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.ArrayList

class GetQuerryManager {
  val gson =new Gson()
  val paquetFilePath ="C:\\Users\\Some Usage\\IdeaProjects\\großeDateien\\fullData.paquet"
  val jsonFilePath= "C:\\Users\\Some Usage\\IdeaProjects\\großeDateien\\cleanedData.json"
  val spark = SparkSession.builder.appName("jsonToParquet").config("spark.master", "local").getOrCreate()

  def getDataframeWithoutSchema(readMode: String): DataFrame = {
    readMode match {
      case "parquet" => return spark.read.parquet(paquetFilePath)
      case "json" => return spark.read.json(jsonFilePath)
    }
  }
  def getDataframeWithSchema(schema:StructType, readMode:String): DataFrame ={
    readMode match {
      case "parquet" => return spark.read.schema(schema).parquet(paquetFilePath)
      case "json" => return spark.read.schema(schema).json(jsonFilePath)
    }
  }

  def getDataframe(query:String,readMode:String,computationMode:String):DataFrame={
    if (computationMode=="sql" ){
      return  getDataframeWithoutSchema(readMode)
    }
    query match {
      case "distinctAuthors" =>
        val schema = new StructType()
          .add("authors", ArrayType(new StructType().add("id", LongType)))
        return getDataframeWithSchema(schema, readMode)
      case "countArticle" =>
        val schema = new StructType().add("id", LongType, false)
        return getDataframeWithSchema(schema, readMode)
    }


  }

  def mostArticles(readMode:String,computationMode:String): List[Author] ={
    computationMode match {
      case "api" =>
        val schema = new StructType()
          .add("id",LongType)
          .add("authors", ArrayType(new StructType().add("id", LongType).add("org",StringType).add("name",StringType)))
        val dataFrame = getDataframeWithSchema(schema, readMode)
        val dataFrameAuthors = dataFrame.select(col("id"),explode(col("authors")).as("authors"))
        val dataFrameArticleToAuthors=dataFrameAuthors.select(col("id").as("articleId"),col("authors.*"))
        val dataFrameGrouped =dataFrameArticleToAuthors.groupBy(col("id")).count()
        dataFrameGrouped.show()
        val highestValue=dataFrameGrouped.agg({"count"-> "max"}).collect()(0)
        dataFrameGrouped.where("count=="+highestValue(0)).show()
        //dataFrameGrouped.orderBy("count").show()
        //dataFrameAuthors.groupBy("col").
        return List.apply()
      case "sql" =>
        return List.apply()
    }
  }

  def distinctAuthors(readMode:String,computationMode:String): Long = {
        computationMode match {
          case "api" =>
            val schema = new StructType()
              .add("authors", ArrayType(new StructType().add("id", LongType)))
            val dataFrame = getDataframeWithSchema(schema,readMode)
            val dataFrameAuthors = dataFrame.select(explode(col("authors")))
            val dataframeDistincts = dataFrameAuthors.distinct()
            return dataframeDistincts.count()
          case "sql" =>
            val dataFrame = getDataframeWithoutSchema(readMode)
            dataFrame.show
            dataFrame.createTempView("completeData")
            val onlyArticles = spark.sql(
              """SELECT authors
                  |FROM  completeData
              """.stripMargin)
            onlyArticles.show()
            val articleIDTable=onlyArticles.select(explode(col("authors")("id")))
            articleIDTable.show()
            articleIDTable.createTempView("unpackedAuthorIds")
            return spark.sql(
              """SELECT COUNT(DISTINCT col)
                 |FROM unpackedAuthorIds
                          """.stripMargin).take(1)(0).getAs[Long](0)
        }

  }


  def countArticle(readMode:String,computationMode:String): Long = {
    computationMode match {
      case "api" =>
        val schema = new StructType().add("id", LongType, false)
        val dataFrame = getDataframeWithSchema(schema,readMode)
        return dataFrame.select(col("id")).count()

      case "sql" => val tableName = "articlesParquetSql"
        val dataFrame = getDataframeWithoutSchema(readMode)
        dataFrame.createTempView(tableName)
        return getCountWithSQL(tableName)
    }
  }



def getCountWithSQL(tableName:String):Long={
  val result= spark.sql(
    ("""
      |SELECT COUNT (id)
      |FROM """ + tableName).stripMargin).take(1)(0).getAs[Long](0)
  return result
}





}
