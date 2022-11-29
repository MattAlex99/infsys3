import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    //create and fill db. only needs to be done once
    //val writingManager = new WritingManager
    //Remove leading commas. Only need to be done once so it is commented out
    //writingManager.rewriteJsonFile()

    //writingManager.splitCompleteDS()
    //writingManager.readJsonAndWritwAsParquet()

    //writingManager.processFileBatchWise("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json/dblp.v12.json")




    val getQuerryManager= new  GetQuerryManager

    val timestamp0: Long = System.currentTimeMillis / 1000

    //println("count by parquet/api: "+getQuerryManager.countArticle("parquet","api")) //takes 1.1/6 seconds
    //println("count by parquet/sql: "+getQuerryManager.countArticle("parquet","sql")) //takes 1.9/6 second
    //println("count by json/sql: "+getQuerryManager.countArticle("json","sql")) //takes 20/22 seconds
    //println("count by json/api "+getQuerryManager.countArticle("json","api")) //takes 15.1/20 seconds

    //println("DistinctAuthors by parquet/api: "+getQuerryManager.distinctAuthors("parquet","api")) //takes 23 seconds
    //println("DistinctAuthors by parquet/sql: "+getQuerryManager.distinctAuthors("parquet","sql")) //25 seconds
    //println("DistinctAuthors by json/api: "+getQuerryManager.distinctAuthors("json","api")) //takes 55 seconds
    //println("DistinctAuthors byjson/sql: "+getQuerryManager.distinctAuthors("json","sql")) //takes 57 seconds

    //println("Most Articles by parquet/api: "+getQuerryManager.mostArticles("parquet","api").mkString(",")) //takes 54 seconds
    //println("Most Articles by parquet/sql: "+getQuerryManager.mostArticles("parquet","sql").mkString(",")) //takes 40sec
    //println("Most Articles by json/api: "+getQuerryManager.mostArticles("json","api").mkString(","))//takes 143 seconds
    //println("Most Articles by json/sql: "+getQuerryManager.mostArticles("json","sql").mkString(",")) //takes 108 seconds

    val timestamp1: Long = System.currentTimeMillis / 1000
    print("operation took: ", timestamp1-timestamp0 , " seconds")


    //keep web UI alive after execution
    System.in.read()
    getQuerryManager.spark.stop()






    }
}