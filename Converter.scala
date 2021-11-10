package com.akkarestload.client

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import java.io._
import java.nio.file.{Files, Paths}
import java.util.Base64

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

/**
  * A converter to perform stream operations
  */
object Converter {

  implicit val system = ActorSystem("system")
  implicit val executionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val BUF_LEN = 72
  val b64coder = Base64.getEncoder
  val b64decoder = Base64.getDecoder
  val chunkSize = 2000
  val waveSize = 24


  def encodeStream (in: InputStream, out: BufferedWriter): Unit = {

    @tailrec
    def continue(buf: Array[Byte]) {
      val read = in.read(buf, 0, buf.length)
      if (read != -1) {
        val s = b64coder.encodeToString(if(read < buf.length) buf.take(read) else buf)
        out.write(s)
        continue(buf) // recur
      }
      // else do not recur = break the loop
    }

    var buffer = new Array[Byte](BUF_LEN/4*3)
    continue(buffer)
  }

  def encodeBase64String(in: ByteString): String = {

    val b64String = b64coder.encodeToString(in.toArray)

    //ByteString(b64String)
    b64String
  }

  def filetobase64(): Unit = {
    val filename = "input/homes.csv"
    val outfilename = "output/homes.csv.b64"
    var buffer = new Array[Byte](BUF_LEN/4*3)

    try {
      val in = new BufferedInputStream(new FileInputStream(filename))
      val out = new BufferedWriter(new FileWriter(outfilename))
      encodeStream(in, out)
      out.flush()
      out.close()
      in.close()

    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
      }
    }

  }

  def main1(args: Array[String]): Unit = {
    println("Hello World!! -- welcome to Converter")
    val filename = "input/employee.txt"
    val outfilename = "output/employee.txt.b64"
    val byteArray = Files.readAllBytes(Paths.get(filename))
    println("All reads buf length: " + byteArray.length)
    println(Base64.getEncoder.encodeToString(byteArray))
    var buffer = new Array[Byte](BUF_LEN/4*3)

    try {

      val bis = new BufferedInputStream(new FileInputStream(filename))
      val bArray = Stream.continually(bis.read(buffer)).takeWhile(-1 !=).map(_.toByte).toArray
      println("Stream reads buf length: " + bArray.length)

      val in = new BufferedInputStream(new FileInputStream(filename))
      val out = new BufferedWriter(new FileWriter(outfilename))
      encodeStream(in, out)
      out.flush()
      out.close()
      in.close()

    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
      }
    }


    //Sm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0
    //Sm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0
    //Sm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0AAAAAAAA
    //Sm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0CkpvaG4gLSBTYWxlc21hbgpUb20gLSBFbmdpbmVlcgpNaWtlIC0gUGhhcm1hY2lzdApKb2huIC0gU2FsZXNtYW4KVG9tIC0gRW5naW5lZXIKTWlrZSAtIFBoYXJtYWNpc3QKSm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0CkpvaG4gLSBTYWxlc21hbgpUb20gLSBFbmdpbmVlcgpNaWtlIC0gUGhhcm1hY2lzdApKb2huIC0gU2FsZXNtYW4KVG9tIC0gRW5naW5lZXIKTWlrZSAtIFBoYXJtYWNpc3QKSm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0
    //Sm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0CkpvaG4gLSBTYWxlc21hbgpUb20gLSBFbmdpbmVlcgpNaWtlIC0gUGhhcm1hY2lzdApKb2huIC0gU2FsZXNtYW4KVG9tIC0gRW5naW5lZXIKTWlrZSAtIFBoYXJtYWNpc3QKSm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0CkpvaG4gLSBTYWxlc21hbgpUb20gLSBFbmdpbmVlcgpNaWtlIC0gUGhhcm1hY2lzdApKb2huIC0gU2FsZXNtYW4KVG9tIC0gRW5naW5lZXIKTWlrZSAtIFBoYXJtYWNpc3QKSm9obiAtIFNhbGVzbWFuClRvbSAtIEVuZ2luZWVyCk1pa2UgLSBQaGFybWFjaXN0
  }

  def main2(args: Array[String]): Unit = {

    val lineStream = FileIO
      .fromPath(Paths.get("output/employee.txt.b64"))
      .via(new Chunker(chunkSize))
      .map(_.utf8String)
      //.runWith(Sink.seq)
      //.foreach(println)

    lineStream.runWith(Sink.seq).foreach(println)

  }

  def main3(args: Array[String]): Unit = {
    val source: Source[Int, NotUsed] = Source(1 to 100)
    //source.runForeach(i => println(i))
    val done: Future[Done] = source.runForeach(i => println(i))
    done.onComplete(_ => system.terminate())
  }

  def main4(args: Array[String]): Unit = {
    val source: Source[Int, NotUsed] = Source(1 to 100)

    val factorials = source.scan(BigInt(1))((acc, next) => acc * next)

    val result: Future[IOResult] =
      factorials.map(num => ByteString(s"$num\n")).runWith(FileIO.toPath(Paths.get("output/factorials.txt")))

    factorials
      .zipWith(Source(0 to 100))((num, idx) => s"$idx! = $num")
      .throttle(1, 1.second)
      .runForeach(println)
  }


  def main5(args: Array[String]): Unit = {
    Range(1,10).foreach(x => {val i=Random.nextInt(10); val f=Random.nextFloat(); println(s"Range Next Int: $i  Next Float: $f ")})

    System.exit(0)
  }


  def main6(args: Array[String]): Unit = {
    //filetobase64()

    val events: Source[Int, NotUsed] = Source(1 to 10)
    val rnd = new Random()

    def eventHandler(event: Int): Future[Int] = {
      println(s"Processing event $event...")
      Future { Thread.sleep(rnd.nextInt(20)*1000L); println(s"Completed processing $event"); event }
    }

    def runRequest(in: Int): Future[String] = {

      val req = HttpRequest(
          method = HttpMethods.GET,
          uri = "http://localhost:8080/echo"
      )

      Http()
        .singleRequest(req)
        .flatMap { response =>
          response.entity.toStrict(2 seconds).map(_.data.utf8String + " -- " + in.toString)
        }
    }

    def runRequestBase64(param: String): Future[String] = {

      val req = HttpRequest(
        method = HttpMethods.GET,
        uri = "http://localhost:8080/echo"
      )

      Http()
        .singleRequest(req)
        .flatMap { response =>
          response.entity.toStrict(2 seconds).map(_.data.utf8String)
        }
    }

    val t0 = System.nanoTime()

//    FileIO
//      .fromPath(Paths.get("output/homes.csv.b64"))
//      .via(new Chunker(chunkSize))
//      .map(_.utf8String)
//      .mapAsync(5) { in =>
//        runRequestBase64(in)
//      }
//      //.map(s => b64decoder.decode(s))
//      .map(ds => ByteString(ds))
//      .runWith(FileIO.toPath(Paths.get("output/homes.csv.dec")))

//    val linestream1 = FileIO
//      .fromPath(Paths.get("input/employee.txt"))
//      .via(new Chunker(chunkSize))
//      .map(_.utf8String)


//    lineStream.runWith(Sink.seq).foreach(println)


    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")


//    events
//      .mapAsync(5) { in =>
//        runRequest(in)
//      }
//      .map { in =>
//        println(s"`mapAsync` emitted event number: $in")
//      }
//      .runWith(Sink.seq)

    //System.exit(0)

  }

  def main8(args: Array[String]): Unit = {
    def runRequestBase64(param: String): Future[String] = {

      val req = HttpRequest(
        method = HttpMethods.GET,
        uri = s"http://localhost:8080/echo?b64str=$param"
      )

      Http()
        .singleRequest(req)
        .flatMap { response =>
          response.entity.toStrict(30 milliseconds).map(_.data.utf8String)
        }
    }


    val t0 = System.nanoTime()

    val done: Future[IOResult] = FileIO
      .fromPath(Paths.get("output/homes.csv.b64"))
      .via(new Chunker(chunkSize))
      .map(_.utf8String)
      .mapAsync(waveSize) { in =>
        runRequestBase64(in)
      }
      .map(s => {val split = if (s.split(",").length == 1) ""  else s.split(",")(1); new String(b64decoder.decode(split))})
      .map(ds => ByteString(ds))
      .runWith(FileIO.toPath(Paths.get("output/homes.csv.dec")))


    done.onComplete(_ => {
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns " + (t1-t0)/1000000000 + " seconds " )
      system.terminate()
    })

  }


  def main(args: Array[String]): Unit = {
    def runRequestBase64(param: String): Future[String] = {

      val req = HttpRequest(
        method = HttpMethods.GET,
        uri = s"http://localhost:8080/echo?b64str=$param"
      )

      Http()
        .singleRequest(req)
        .flatMap { response =>
          response.entity.toStrict(30 milliseconds).map(_.data.utf8String)
        }
    }


    val t0 = System.nanoTime()

//    val done: Future[IOResult] = FileIO
//      .fromPath(Paths.get("input/homes.csv"))
//      .via(new Chunker(4080))
//      .map(encodeBase64String)
//      //.map(_.utf8String)
//      //.mapAsync(waveSize) { in =>
//      //  runRequestBase64(in)
//      //}
//      //.map(s => {new String(b64decoder.decode(s))})
//      .map(ds => ByteString(ds + "\n"))
//      .runWith(FileIO.toPath(Paths.get("output/homes.csv.b64.chunk")))
//
//
//    done.onComplete(_ => {
//      val t1 = System.nanoTime()
//      println("Elapsed time: " + (t1 - t0) + "ns " + (t1-t0)/1000000 + " ms " )
//      system.terminate()
//    })

    val t2 = System.nanoTime()

    val decdone: Future[IOResult] = FileIO
      .fromPath(Paths.get("output/homes.csv.b64.chunk"))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 8192))
      //.via(Framing.delimiter(ByteString("\n"), 4080, true))
      .map(_.utf8String)
      //.mapAsync(waveSize) { in =>
      //  runRequestBase64(in)
      //}
      .map(s => {new String(b64decoder.decode(s))})
      .map(ds => ByteString(ds))
      .runWith(FileIO.toPath(Paths.get("output/homes.csv.dec.chunk")))


    decdone.onComplete(_ => {
      val t3 = System.nanoTime()
      println("Elapsed time: " + (t3 - t2) + "ns " + (t3-t2)/1000000 + " ms " )
      system.terminate()
    })

  }



}
