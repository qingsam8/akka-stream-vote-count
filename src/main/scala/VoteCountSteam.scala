import java.nio.file.Paths
import java.util.Calendar

import akka.actor.{ActorSystem}
import akka.stream.scaladsl._
import akka.stream.{IOResult, ActorMaterializer}
import akka.util.ByteString

import scala.collection.parallel.{ParMap, ParSeq}
import scala.concurrent.Future
import scala.util.Success


object VoteCountSteam extends App{

  implicit val system = ActorSystem("system")
  implicit val mat = ActorMaterializer()
  implicit val ex = system.dispatcher


  var db: Map[String, Int] =  Map()

  /**
    * updates the map with the current value
    *
    * @param map
    */
  def updateCount(map : ParMap[String, Int]) = {
    map.toList.foreach(vote => {
      val number: Int  = db.get(vote._1).getOrElse(0)
      db = db.updated(vote._1, vote._2+ number)
    })
  }

  /**
    * Gives a token to a value
    * @param value
    * @tparam T
    * @return
    */
  def tokenize[T](value: T): Future[(T, Int)] = Future{(value, 1)}

  def groupVotes(stringList : Seq[String]): Future[ParMap[String, ParSeq[String]]] =
    Future {
      stringList.par.groupBy(_.toString)
    }

  def countVotes(voteMap: ParMap[String, ParSeq[String]]) = {
    Future {
      voteMap.map({
        case (k, v) => k -> v.length
      })
    }
  }


  //**********************************************************

  val fileName = "vote1mil"
  val dataFileGen =  new DataFileGen
  dataFileGen.createDataFile(fileName, 1000000)
  val file = Paths.get(fileName)


  val calendar = Calendar.getInstance();
  val start  = calendar.getTime()
  println("Start: " + start)
  val future = FileIO.fromPath(file).
    via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 100, allowTruncation = true))
    .map(_.utf8String)
    .filterNot(s => s.isEmpty)
    .grouped(10000)
    .mapAsyncUnordered(3)(groupVotes)
    .mapAsyncUnordered(3)(countVotes)
    .runForeach(map => updateCount(map))


  future.onComplete({
    case Success(x) => {
      val tt = Calendar.getInstance().getTime()
      println("Complete: " + Calendar.getInstance().getTime())
      println(db.foldLeft(0)((a, b) => a+b._2) + " votes processed in: "  + (tt.getTime() -  start.getTime())/1000 + "ms")
      system.terminate()
    }
  })



}


