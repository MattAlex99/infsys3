import com.google.gson.Gson
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.StringType
import scala.annotation.tailrec
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



  def mostArticles(readMode:String,computationMode:String): List[Author] ={
    computationMode match {
      case "api" =>
        val schema = new StructType()
          .add("authors", ArrayType(new StructType().add("id", LongType).add("org",StringType).add("name",StringType)))
        val dataFrame = getDataframeWithSchema(schema, readMode)
        //lets get a dataframe that lists all citations (article id to authorId,name,org)
        val dataFrameAuthors = dataFrame.
          select(explode(col("authors")).as("authors"))
        val dataFrameArticleToAuthors=dataFrameAuthors
          .select(col("authors.*"))
        val dataFrameGrouped =dataFrameArticleToAuthors
          .groupBy(col("id")).count()

        //get the list of all authors that have the highest count
        val highestValue=dataFrameGrouped.
          agg({"count"-> "max"}).collect()(0)
        val dataFrameHighestCountIDs= dataFrameGrouped.
          where("count=="+highestValue(0)).select(col("id"))
        val listOfAuthors= getListOfIdFromRowArray(dataFrameHighestCountIDs.collect(),0)

        //clean list with all citations so we have only one entry for each author
        val authorIdToInfoUnique = dataFrameArticleToAuthors.
          dropDuplicates("id")

        val resultDF= authorIdToInfoUnique.
          filter(col("id").isin(listOfAuthors:_*))
        return getListOfAuthorsFromRowArray(resultDF.collect())

      case "sql" =>
        //read a dataframe ...
        val schema = new StructType()
          .add("authors", ArrayType(new StructType().add("id", LongType).add("org", StringType).add("name", StringType)))
        val dataFrame = getDataframeWithSchema(schema, readMode)
        //... and transform it so that we get a table with columns id,name,org
        val authorsExplodedDF = dataFrame.select(explode(col("authors")).as("authors"))
        val authorCitationsDF = authorsExplodedDF.select(col("authors.*"))
        //select all Ids of authors with the highest
        authorCitationsDF.createTempView("AuthorCitations")
        val authorIDs = spark.sql(
          """Select id ,Min(name),Min(org) FROM (
              |SELECT id , name , org FROM AuthorCitations WHERE id in (
              |SELECT distinct id FROM AuthorCitations
              |GROUP BY id
              |having count(id)=(SELECT MAX(article)
              |FROM (
                |SELECT id, COUNT(id) as article
              |FROM AuthorCitations
              |GROUP BY id)))
              |) GROUP BY id""".stripMargin).collect()
       //get the name and org for all authors with high score
        val authorIDStrings = authorIDs.mkString(", ")
        print(authorIDStrings)
        val result = getListOfAuthorsFromRowArray(authorIDs)
        return result
    }
  }


  def getListOfIdFromRowArray(inputArray:Array[Row], iter:Integer=0, resultList:List[Long]=List()):List[Long]={
    return inputArray.map(entry => entry.getAs("id").asInstanceOf[Long] ).toList
  }


  final def getListOfAuthorsFromRowArray(inputArray: Array[Row]): List[Author] = {
    return inputArray.map(entry => Author(entry.get(0).asInstanceOf[Long],
      entry.get(1).asInstanceOf[String],
      entry.get(2).asInstanceOf[String])).toList
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
            val schema = new StructType()
              .add("authors", ArrayType(new StructType().add("id", LongType)))
            val dataFrame = getDataframeWithSchema(schema, readMode)
            val articleIDTable=dataFrame.select(explode(col("authors")("id")))

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

      case "sql" =>
        val tableName = "articlesParquetSql"
        val schema = new StructType().add("id", LongType, false)
        val dataFrame = getDataframeWithSchema(schema, readMode)
        dataFrame.createTempView(tableName)
        return spark.sql(
          ("""
             |SELECT COUNT (id)
             |FROM """ + tableName).stripMargin).take(1)(0).getAs[Long](0)

    }
  }









}
