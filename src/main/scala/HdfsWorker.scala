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

  def counts[String](xs: Iterator[String]): mutable.Map[String, Int] = {
    xs.foldLeft(mutable.HashMap.empty[String, Int].withDefaultValue(0))(
      (acc, x) => {
        acc(x) += 1; acc
      })
  }

  def writeToFile(map: mutable.Map[String, Int], fileName: Path) = {
    val writer = new PrintWriter(fs.create(fileName))
    for (line <- map) {
      writer.write(line + "\n")
    }
    writer.close()
  }
}
//  53289330
