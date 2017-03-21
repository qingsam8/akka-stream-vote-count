import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.{IOResult, ActorMaterializer}
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.{Future, Promise}
import scala.util.Random

case class VoteSetting(setting: List[(String, Int, Int)])

class DataFileGen(implicit val system: ActorSystem, implicit val mat: ActorMaterializer){

  import scala.concurrent.ExecutionContext.Implicits.global

//  private val r = Random

  def createVotes(setting: (String, Int, Int)): Future[String] = {
    Future {
      println(setting)
      val list = (1 to setting._2).toList
      list.map(x => setting._1 + ", client" +  x +"\n")
        .foldLeft("")((string, ele) => string + ele)
    }
  }

  def lineSink(filename: String): Sink[String, Future[IOResult]] =
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)


  def createDataFile(fileName: String, numOfVotes: Int) = {
    val f = new File(fileName)
    if (f.exists()) {
      println(s"Text file $fileName exist")
    }
    else {
      val tt = Source(1 to numOfVotes)
        .map(x => {
          "candidate1, client" + x
        })
        .runWith(lineSink(fileName))
      tt.onComplete(io => {
      })
    }
  }





}
