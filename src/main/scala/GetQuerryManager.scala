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
        //lets get a dataframe that lists all citations (article id to authorId,name,org)
        val dataFrameAuthors = dataFrame.select(explode(col("authors")).as("authors"))
        val dataFrameArticleToAuthors=dataFrameAuthors.select(col("authors.*"))
        val dataFrameGrouped =dataFrameArticleToAuthors.groupBy(col("id")).count()
        //get the list of all authors that have the highest count
        val highestValue=dataFrameGrouped.agg({"count"-> "max"}).collect()(0)
        val dataFrameHighestCountIDs= dataFrameGrouped.where("count=="+highestValue(0)).select(col("id"))
        //val columnOfMostId= dataFrameHighestCountIDs.collect()
        val listOfAuthors= getListOfIdFromRowArray(dataFrameHighestCountIDs.collect(),0)
        //clean list with all citations so we have only one entry for each author
        val authorIdToInfoUnique = dataFrameArticleToAuthors.dropDuplicates("id")
        //
        val resultDF= authorIdToInfoUnique.filter(col("id").isin(listOfAuthors:_*))

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
          """SELECT id FROM AuthorCitations
            |GROUP BY id
            |having count(id)=(SELECT MAX(article)
            |FROM (
            |SELECT id, COUNT(id) as article
            |FROM AuthorCitations
            |GROUP BY id))""".stripMargin).collect()
       //get the name and org for all authors with high score
        val authorIDStrings = authorIDs.mkString(", ")
        val authorArray = spark.sql("SELECT id , name , org FROM Authors WHERE id in (" + authorIDStrings.substring(1, authorIDStrings.length - 1) + ")").distinct().collect()
        return getListOfAuthorsFromRowArray(authorArray)
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
