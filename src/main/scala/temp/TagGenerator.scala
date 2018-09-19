package temp

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}




object TagGenerator {
  def main(args: Array[String])= {
    val file = new File("D:/tmp/tagsgen/result")
    deleteDir(dir = file )

    val conf = new SparkConf().setAppName("taggen").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val poi_tags = sc.textFile("file:///D:/tmp/tagsgen/temptags.txt") // to be replaced by hive data resources
    val poi_taglist = poi_tags.map(e=>e.split("\t")).filter(e=>e.length == 2).
      map(e=>e(0)->ReviewTags.extractTags(e(1))).
      filter(e=> e._2.length > 0).map(e=> e._1 -> e._2.split(",")).
      flatMapValues(e=>e).
      map(e=> (e._1,e._2)->1).
      reduceByKey(_+_).
      map(e=> e._1._1 -> List((e._1._2,e._2))).
      reduceByKey(_:::_).
      map(e=> e._1-> e._2.sortBy(_._2).reverse.take(10).map(a=> a._1+":"+a._2.toString).mkString(","))
    poi_taglist.map(e=>e._1+"\t"+e._2).saveAsTextFile("file:///D:/tmp/tagsgen/result")
  }

  //先删除文件夹 !!!!!!22222
  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }
}
