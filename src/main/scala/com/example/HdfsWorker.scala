package com.example

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

object HdfsWorker extends App {


  var path = new Path("/user/ilia/firstTask")
  println("Start working, path is: "+path.toString)

  if (args.length == 1)
    path = new Path(args(0))

  val conf = new Configuration()

  val fs = FileSystem.get(conf)

  val files = fs.listStatus(path)

  var map = mutable.HashMap[String, Int]().withDefaultValue(0)
  val p = "^.*?(?=\\t)".r
  for {
    file <- files
    lines = Source
      .fromInputStream(fs.open(file.getPath))
      .getLines()
      .map(line => line.split("\t")(2))
      .foldLeft(mutable.HashMap.empty[String, Int].withDefaultValue(0))((acc, x) => {acc(x) += 1; acc})
  } yield {
    println("looking file: " + file.getPath.toString)
    val newPath = file.getPath.toString.split(".bid.")(0) + "/result/"
    writeToFile(lines, new Path(newPath + file.getPath.toString.split(".bid.")(1)))
    lines.clear()
    println(file.getPath.toString)
  }

  path = new Path(path.toString + "/result/")
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

  writeToFile(map.toSeq.sortWith(_._2 > _._2), new Path(path + "/result.txt"))

  def counts[String](xs: Iterator[String]): mutable.Map[String, Int] = {
    xs.foldLeft(mutable.HashMap.empty[String, Int].withDefaultValue(0))(
      (acc, x) => {
        acc(x) += 1; acc
      })
  }

  def writeToFile(seq: Seq[(String, Int)], fileName: Path)(implicit ec: ExecutionContext) = {
    val writer = new PrintWriter(fs.create(fileName))
    for (line <- seq) {
      writer.write(line + "\n")
    }
    writer.close()
  }
  def writeToFile(map: mutable.Map[String, Int], fileName: Path)(implicit ec: ExecutionContext) = {
    val writer = new PrintWriter(fs.create(fileName))
    for (line <- map) {
      writer.write(line + "\n")
    }
    writer.close()
  }
}
//  53289330
