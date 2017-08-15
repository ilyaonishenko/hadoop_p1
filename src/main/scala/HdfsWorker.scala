import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable
import scala.io.Source

object HdfsWorker extends App {

  var path = new Path("/training/")

  if (args.length == 1)
    path = new Path(args(0))

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  val files = fs.listStatus(path)

  var map = mutable.HashMap[String, Int]().withDefaultValue(0)
  for {
    file <- files
    lines = Source
      .fromInputStream(fs.open(file.getPath))
      .getLines()
      .map(line => line.split("\t")(0))
  } yield {
    println("looking file: " + file.getPath.toString)
//    lines.foreach(x => map(x) += 1)
    map = counts(lines)
    val newPath = file.getPath.toString.split(".bid.")(0) + "/result/"
    writeToFile(map,
                new Path(newPath + file.getPath.toString.split(".bid.")(1)))
    map.clear()
  }

  path = new Path("/training/result/")
  val results = fs.listStatus(path)
  val pattern = "\\((.*),([^,]+)\\)$".r

  for {
    file <- results
    lines = Source
      .fromInputStream(fs.open(file.getPath))
      .getLines()
      .map(line => { val pattern(key, value) = line; (key, value) })
  } yield {
    println("looking file2: " + file.getPath.toString)
    lines.foreach(line => map(line._1) += line._2.toInt)
  }

  writeToFile(map, new Path(path + "/result.txt"))

  //  var mainMap: Map[String, Int] = Map()
  //
  //  var iter: Iterator[String] = Iterator()

  /*var globalCounter = 0

  val map = mutable.HashMap[String, Int]().withDefaultValue(0)
  var line = ""
  for {
    file <- files
    reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath)), 64)
  } yield {
    reader.lines().forEach(line => map(line.split("\t")(2)) += 1)
    writeToFile(map.toSeq, new Path(file.getPath + "output.txt"))
    map.clear()
  }*/
  //  writeToFile(map.toSeq, new Path(path.toString + "/output.txt"))

  //  for (file <- files) {
  //    val iter = Source
  //      .fromInputStream(fs.open(file.getPath))
  //      .getLines()
//      .map(_.split("\t")(2))
//
//    lazy val toSeq = counts(iter).toSeq
//    writeToFile(toSeq, new Path(file.getPath + "res.txt"))
//    println(file.getPath.getName + "--------------" + toSeq.size)
//    mainMap = mainMap ++ miniMap

//    while(lines.hasNext){
//      lines.toStream.groupBy()
//    }
//    println("done")

//  writeToFile(mainMap.toSeq, new Path(path.toString+"/output.txt"))

  def counts[String](xs: Iterator[String]): mutable.Map[String, Int] = {
    xs.foldLeft(mutable.HashMap.empty[String, Int].withDefaultValue(0))(
      (acc, x) => {
        acc(x) += 1; acc
      })
  }

  //  val func: java.util.function.Function[String, String] = line => line.split("\t")(3)
  //  val mainMap: Map[String, Int] = Map()
  /*for (file <- files) {
    val idSeq: util.Map[String, lang.Long] =
      new BufferedReader(new InputStreamReader(fs.open(file.getPath)))
        .lines().collect(Collectors.groupingBy( func, Collectors.counting()))
    val writer = new PrintWriter(fs.create(new Path(file.getPath+"res.txt")))
    for (line <- idS) {
      writer.write(line + "\n")
    }
    writer.close()
  }*/

  /*for(file <- files){
    new BufferedReader(new InputStreamReader(fs.open(file.getPath)))
  }*/

  def writeToFile(map: mutable.Map[String, Int], fileName: Path) = {
    val writer = new PrintWriter(fs.create(fileName))
    for (line <- map) {
      writer.write(line + "\n")
    }
    writer.close()
  }

//  val sortedMap: Seq[(String, Int)] = mainMap.toSeq.sortWith(_._2 < _._2)
}
//"/training/bid_result.txt"
//  for (file <- files) {
//    val bufferedReader = new BufferedReader(
//      new InputStreamReader(fs.open(file.getPath)))
//    while ({
//      line = bufferedReader.readLine; line != null
//    }) {
//      val arr = line.split("\t")
//      map.ad
//    }
//  }
//  for (file <- files) {
//    val bufferedReader = new BufferedReader(new InputStreamReader(fs.open(file.getPath)))
//    while ( {
//      line = bufferedReader.readLine; line != null
//    }) {
//      counter += 1
//    }
//  }
//  53289330

//def readStrings(path: Path, configuration: Configuration): List[String] = {
//
//  val fs = FileSystem.get(path.toUri, configuration)
//  val factory = new CompressionCodecFactory(configuration)
//  val items = fs.listStatus(path)
//
//  import java.io.StringWriter
//  for (item <- items) { // ignoring files like _SUCCESS
//    val codec = factory.getCodec(item.getPath)
//    var stream = null
//    // check if we have a compression codec we need to use
//    if (codec != null)
//      stream = codec.createInputStream(fs.open(item.getPath))
//    else
//      stream = fs.open(item.getPath)
//    val writer = new StringWriter
//    IOUtils.copy(stream, writer, "UTF-8")
//    val raw = writer.toString
//    val resulting = raw.split("\n")
//    for (str <- raw.split("\n")) {
//      results.add(str)
//    }
//  }
//}
