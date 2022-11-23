import com.google.gson.Gson
import java.util

object StringProcessing {
  val gson = new Gson()

  def cleanAuthorString(articleString: String, articleList: util.ArrayList[Article]): Unit = {
    val rawLineString = articleString.replaceAll("[^\\x00-\\x7F]", "?")
    rawLineString match {
      case "]" =>
      case "[" =>
      case _ => val clearedLineString = removeLeadingComma(rawLineString)
        articleList.add(gson.fromJson(clearedLineString, classOf[Article]))
    }
  }

  final private def removeLeadingComma(string: String): String = {
    string.take(1) match {
      case "," => removeLeadingComma(string.substring(1))
      case _ => string
    }
  }

}
