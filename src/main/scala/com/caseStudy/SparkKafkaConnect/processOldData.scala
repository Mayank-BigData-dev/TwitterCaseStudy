import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

object processOldData {

  def filterdata(str: String): String ={
    val parser_ = new JSONParser()
    val object_ = parser_.parse(str)
    val tweet_Object = object_.asInstanceOf[JSONObject]

    val created = tweet_Object.get("created").asInstanceOf[String]
    val location = tweet_Object.get("location").asInstanceOf[String]
    val text = tweet_Object.get("text").asInstanceOf[String]
    val oid = tweet_Object.get("_id").asInstanceOf[JSONObject]
    val stopWords = Set("a","an","is","was","the","to","RT","of","in","and","are","on","de","for","","The","that","this","la","el","en","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out",
      "very", "having", "with", "they", "own", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself",
      "other", "off", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "we", "these",
      "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above",
      "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will",
      "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just",
      "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "by", "doing", "it",
      "how", "further", "here", "than", "", "eu", "gosto", "der" ,"RT")

    val text_emoji_filtered = text.replaceAll("[-+.^!?:=;%,&\"(\\u00a9|\\u00ae|[\\u2000-\\u3300]|\\ud83c[\\ud000-\\udfff]|\\ud83d[\\ud000-\\udfff]|\\ud83e[\\ud000-\\udfff])]","")

    val filteredText = text_emoji_filtered.toLowerCase.split(" ").distinct.filterNot(x => stopWords.contains(x)).filter(x => x.matches("[a-zA-Z]*$")).filter(x => x.length >2).mkString(" ")

    val wordsArray = filteredText.split(" ")


    val dateString = "$date"

    val createdString = created+"T00:00:00.000+00:00"


    val ArrayString = wordsArray.mkString("\", \"")
    val finalArrayString = "["+"\""+ArrayString+"\""+"]"
    println(finalArrayString)



    return (s"""{"_id" : $oid , "created" : { "$dateString": \"$createdString\" } , "location" : \"$location\" , "text" : $finalArrayString}""")

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val pwText = new FileWriter("/Users/mayank/Desktop/looseoutput.json")
    val sc = new SparkContext("local[*]","processOldData")
    val data = sc.textFile("/Users/mayank/Desktop/loose.json",3)
    val lines = data.collect()


    pwText.write("[")

    lines.foreach(x => {
      val output = filterdata(x)
      pwText.write(output+","+"\n")
    })


    pwText.write("]")

    pwText.close()

  }

}
