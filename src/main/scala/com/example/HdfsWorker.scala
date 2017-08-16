package com.example

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

object HdfsWorker extends App {

  var path = new Path("/user/ilia/firstTask")
  println("Start working, path is: " + path.toString)

  if (args.length == 1)
    path = new Path(args(0))

  val conf = new Configuration()

  val fs = FileSystem.get(conf)

  val files = fs.listStatus(path)

  private val map: Map[String, Int] = (for {file <- files} yield {
    Source
      .fromInputStream(fs.open(file.getPath))
      .getLines()
      .map(line => line.split("\t")(2))
      .toList
      .groupBy(x => x)
      .mapValues(_.size)
      .toList
  }).flatten.groupBy(t => t._1).mapValues(list => list.map(t => t._2).sum)

  writeSeqToFile(map.toSeq.sortWith(_._2 > _._2), new Path(path + "/result.txt"))

  def writeSeqToFile(map: Seq[(String, Int)], fileName: Path) = {
    val writer = new PrintWriter(fs.create(fileName))
    for (line <- map) {
      writer.write(line + "\n")
    }
    writer.close()
  }
}
//  53289330
