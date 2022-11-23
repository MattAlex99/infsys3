import com.google.gson.Gson
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.dsl.expressions.windowSpec
import org.apache.spark.sql.functions.{count, countDistinct}
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions._

import scala.annotation.tailrec
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
          .add("authors", ArrayType(new StructType().add("id", LongType).add("org",StringType).add("name",StringType)))
        val dataFrame = getDataframeWithSchema(schema, readMode)
        //lets get a dataframe that lists all citations
        val dataFrameAuthors = dataFrame.select(explode(col("authors")).as("authors"))
        val dataFrameArticleToAuthors=dataFrameAuthors.select(col("authors.*"))
        val dataFrameGrouped =dataFrameArticleToAuthors.groupBy(col("id")).count()
        //get the list of all authors that have the highest count
        val highestValue=dataFrameGrouped.agg({"count"-> "max"}).collect()(0)
        val dataFrameHighestCountIDs= dataFrameGrouped.where("count=="+highestValue(0)).select(col("id"))
        val columnOfMostId= dataFrameHighestCountIDs.collect()
        val listOfAuthors= getListOfIdFromRowArray(columnOfMostId,0)

        val authorIdToInfoUnique = dataFrameArticleToAuthors.dropDuplicates("id")
        val resultDF= authorIdToInfoUnique.filter(col("id").isin(listOfAuthors:_*))

        return getListOfAuthorsFromRowArray(resultDF.collect())

      case "sql" =>
        //val dataFrame = getDataframeWithoutSchema(readMode)
        //dataFrame.show
        //dataFrame.createTempView("completeData")
        //val onlyAuthors = spark.sql(
        //  """SELECT authors
        //    |FROM  completeData
        //  """.stripMargin)
        //val dataFrameArticleToAuthors = onlyAuthors.select(col("authors.*"))
        //dataFrameArticleToAuthors.createTempView("Authors")
        //
        ////change shit here
        //val authorIDs = spark.sql(
        //  """SELECT id FROM Authors
        //    |GROUP BY id
        //    |having count(id)=(SELECT MAX(article)
        //    |FROM (
        //    |SELECT id, COUNT(id) as article
        //    |FROM Authors
        //    |GROUP BY id))""".stripMargin).collect()
        //val authorIDsString = authorIDs.mkString(", ")
        //val authorArray = spark.sql("SELECT id , name , org FROM Authors WHERE id in (" + authorIDsString.substring(1, authorIDsString.length - 1) + ")").distinct().collect()
        //rowArray2authorList(authorArray)
        return List.apply()
    }
  }


  def getListOfIdFromRowArray(inputArray:Array[Row], iter:Integer=0, resultList:List[Long]=List()):List[Long]={
    iter>=inputArray.length match {
      case true => return resultList
      case false =>
        val localRow=inputArray(iter)
        val newLong=localRow.getAs("id").asInstanceOf[Long]
        return getListOfIdFromRowArray(inputArray,iter+1,resultList:+newLong):List[Long]

    }
  }


  @tailrec
  final def getListOfAuthorsFromRowArray(inputArray: Array[Row], iter: Integer=0, resultList: List[Author] = List()): List[Author] = {
    iter >= inputArray.length match {
      case false =>
        val localRow = inputArray(iter)
        val newAuthor = Author (
          localRow.getAs("id").asInstanceOf[Long],
          localRow.getAs("name").asInstanceOf[String],
          localRow.getAs("org").asInstanceOf[String])
          return getListOfAuthorsFromRowArray(inputArray, iter + 1, resultList :+ newAuthor)
      case true => return resultList
    }
  }
  def getAutorFromIdAndDF(id:Int,df:DataFrame):Author={
    val localRow=df.where("id=="+id).collect()(0)
    return Author(localRow.getAs("id").asInstanceOf[Long],
      localRow.getAs("name").asInstanceOf[String],
      localRow.getAs("org").asInstanceOf[String])

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
