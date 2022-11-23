import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    //create and fill db
    val writingManager = new WritingManager
    //Remove leading commas. Only need to be done once so it is comented out
    //writingManager.rewriteJsonFile()

    //writingManager.splitCompleteDS()
    //writingManager.readJsonAndWritwAsParquet()

    //writingManager.processFileBatchWise("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json/dblp.v12.json")




    val getQuerryManager= new  GetQuerryManager
    //println("count by json/api "+getQuerryManager.countArticle("json","api")) //takes 15.1 seconds
    //println("count by parquet/api: "+getQuerryManager.countArticle("parquet","api")) //takes 1.1 seconds

    //println("count by parquet/sql: "+getQuerryManager.countArticle("parquet","sql")) //takes 1.9 second
    //println("count by json/sql: "+getQuerryManager.countArticle("json","sql")) //takes 45 seconds

    //println("DistinctAuthors by parquet/api: "+getQuerryManager.distinctAuthors("parquet","api")) //takes 20 seconds
    //println("DistinctAuthors by parquet/sql: "+getQuerryManager.distinctAuthors("parquet","sql"))

    //println("DistinctAuthors by json/api: "+getQuerryManager.distinctAuthors("json","api"))
    //println("DistinctAuthors byjson/sql: "+getQuerryManager.distinctAuthors("json","sql"))

    println("Most Articles by json/sql: "+getQuerryManager.mostArticles("parquet","api"))

    //keep web UI alive after execution
    System.in.read()
    getQuerryManager.spark.stop()






    }
}