import com.google.gson.Gson
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, element_at, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructType}

import java.io.{FileInputStream, FileWriter}
import java.lang.Integer
import java.nio.charset.StandardCharsets
import java.util
import java.util.Scanner
import java.util.regex.Pattern
import scala.annotation.tailrec

class WritingManager {
    val batchSize=10
    val cleanedDataPath="D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json\\cleanedData.json"

  def rewriteJsonFile()={
    val is = new FileInputStream("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json\\dblp.v12.json")
    val scanner = new Scanner(is, StandardCharsets.UTF_8.name())
    val fw = new FileWriter(cleanedDataPath, true)
    val gson = new Gson()
    while(scanner.hasNextLine){
      val currentLine = scanner.nextLine()
      val cleanedLine = removeLeadingComma(currentLine)
      if (!(currentLine == "[" || currentLine=="]")){
        val currentLineAsObject = gson.fromJson(cleanedLine, classOf[Article])
        val stringFromObject = gson.toJson(currentLineAsObject)
        fw.write(stringFromObject+"\n")
      }
    }
    fw.close()
  }

  def readJsonAndWritwAsParquet()={
    //create authors table
    val fullLineSchema = new StructType()
      .add("id", LongType, false)
      .add("title", StringType, false)
      .add("year", IntegerType, true)
      .add("n_citation", IntegerType, true)
      .add("doc_type", StringType, true)
      .add("page_start", StringType, true)
      .add("page_end", StringType, true)
      .add("publisher", StringType, true)
      .add("issue", StringType, true)
      .add("volume", StringType, true)
      .add("references",ArrayType(LongType))
      .add("authors",ArrayType(new StructType().add("id",LongType).add("org",StringType).add("name",StringType)))
    val spark = SparkSession.builder.appName("jsonToParquet").config("spark.master", "local").getOrCreate()
    val fullLineDS = spark.read.option("multiline","true").schema(fullLineSchema).json("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json\\dblp.v12.json").cache()
    //val fullLineDS = spark.read.option("multiline","true").schema(fullLineSchema).parquet("D:\\Uni\\Master1\\Inf-Sys\\aufgabe3\\fullData.paquet\\part-00000-913b25b5-150a-4590-82b1-482b55a4e92d-c000.snappy.parquet").cache()
    fullLineDS.write.mode("append").parquet("D:\\Uni\\Master1\\Inf-Sys\\aufgabe3\\smallData.paquet")
    fullLineDS.show()
    spark.stop()

  }

  def splitCompleteDS()={
    val spark = SparkSession.builder.appName("jsonToParquet").config("spark.master", "local").getOrCreate()
    val fullLineDS = spark.read.option("multiline","true").parquet("D:\\Uni\\Master1\\Inf-Sys\\aufgabe3\\smallData.paquet").cache()

    //id still needs to be made primary key
    val articleToAuthors=fullLineDS.select(
      col("id"),
      explode(col("authors")("id"))
      )

    val authors = fullLineDS.select(
      explode(col("authors")))
    val authorsToInfo = authors.
      select(col("col")("id"),col("col")("name"),col("col")("org"))

      //.select("id","name","org")
      //.withColumn("id",element_at(col("col"),1)("id"))

    articleToAuthors.show()
    //fullLineDS.write.mode("append").parquet("D:\\Uni\\Master1\\Inf-Sys\\aufgabe3\\fullData.paquet")
    //val AuthorToInfo=fullLineDS.select(col("author").alias("newIdName"),col("publisher"))
  }
    def processFileBatchWise(path:String):Unit={
      val outputPath="D:\\Uni\\Master1\\Inf-Sys\\aufgabe3\\fullDataByBatch.paquet"
      var currentIteration=0
      val is = new FileInputStream(path)
      val gson = new Gson
      val scanner = new Scanner(is, StandardCharsets.UTF_8.name())
      val spark = SparkSession.builder.appName("jsonToParquet").config("spark.master", "local").getOrCreate()

      while (scanner.hasNextLine) {
        val setManager = new GetQuerryManager()
        val currentBatch = getBatches(0, batchSize, new util.ArrayList[String](), scanner)
        //setManager.processBatch(currentBatch,spark)
        println("fetched batch")
        val batchDF= createDataFrameFromBatch(currentBatch,spark)
        println("df was created")
        batchDF.write.mode("append").parquet(outputPath)
        println("batch "+currentIteration+" has been processed")
        currentIteration=currentIteration+1
        println(java.time.LocalDateTime.now().toString)
      }
      //just some finishing Operation
      spark.stop()
    }

  def createDataFrameFromBatch(batch:util.ArrayList[String],spark:SparkSession):DataFrame= {
    val fullLineSchema = new StructType()
      .add("id", LongType, false)
      .add("title", StringType, false)
      .add("year", IntegerType, true)
      .add("n_citation", IntegerType, true)
      .add("doc_type", StringType, true)
      .add("page_start", StringType, true)
      .add("page_end", StringType, true)
      .add("publisher", StringType, true)
      .add("issue", StringType, true)
      .add("volume", StringType, true)
      .add("references", ArrayType(LongType))
      .add("authors", ArrayType(new StructType().add("id", LongType).add("org", StringType).add("name", StringType)))
    return createDataFrameFromBatchInner(0,
      batch,spark.emptyDataFrame,
      fullLineSchema,spark)
  }
  @tailrec
  final def createDataFrameFromBatchInner(iteration:Integer, batch:util.ArrayList[String], dataFrame: DataFrame, struct:StructType, spark:SparkSession):DataFrame={
    val bSize:Integer=batch.size()
    val a:Integer =10
    val b:Integer =0
    println(bSize)
    println(iteration)

    if (iteration==0){
      import spark.implicits._
      print(removeLeadingComma(batch.get(iteration)))
      val df = spark.read.schema(struct).json(Seq(removeLeadingComma(batch.get(iteration))).toDS)
      return createDataFrameFromBatchInner(iteration+1,batch,df,struct,spark)
    }
    else if(iteration==bSize){
      return dataFrame
    }
    else {
      import spark.implicits._
      print(batch.get(iteration))
      val df = spark.read.schema(struct).json(Seq(batch.get(iteration)).toDS)
      df.show()
      val combinedDF = dataFrame.union(df)
      return createDataFrameFromBatchInner(iteration + 1, batch, combinedDF, struct, spark)
    }
    //iteration match{
    //  case bSize =>print("end")
    //    return dataFrame
    //  case b => print("b")
    //    import spark.implicits._
    //      val df = spark.read.schema(struct).json(Seq(batch.get(iteration)).toDS)
    //      df.show()
//
    //      return createDataFrameFromBatchInner(iteration+1,batch,df,struct,spark)
    //  case _ =>print("c")
    //    import spark.implicits._
    //    val df = spark.read.schema(struct).json(Seq(batch.get(iteration)).toDS)
    //    df.show()
    //    val combinedDF = dataFrame.union(df)
    //    return createDataFrameFromBatchInner(iteration+1,batch,combinedDF,struct,spark)
    //}
  }
  def getBatches(currentCount:Integer, batchSize:Integer, currentArticles:util.ArrayList[String], scanner:Scanner):util.ArrayList[String]={
       if (scanner.hasNextLine) {
         if (currentCount == batchSize)
           return currentArticles
         else {
           currentArticles.add(scanner.nextLine())
           return getBatches(currentCount + 1,
             batchSize,
             currentArticles,
             scanner)}

       } else {
         return currentArticles
       }

  }


  @tailrec
  final def removeLeadingComma(string: String):String = {
    string.take (1) match {
    case "," => removeLeadingComma(string.substring (1))
    case _ => string
    }
  }



}
