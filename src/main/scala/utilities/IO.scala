package utilities

import mapReduce.accumulate.{AccumulateMapper, AccumulateReducer}
import mapReduce.average.{AverageMapper, AverageReducer}

object IO {

  def checkArgs(args: Array[String]): Unit = {

    // Notify user that arguments must be used
    if (args.length == 0) {
      println("Arguments required run -help for assistance")
      System.exit(-1)
    }


    // Print help
    if (args(0) == "-help") printHelp()

    run(args)

  }



  def run(args: Array[String]): Unit = {

    args(0).toLowerCase match {
      case "-acc" => {
        checkArgCount(args, 3)
        val job = JobUtil.buildJob(args, args(1).toUpperCase, args(2).toUpperCase)
        job.setMapperClass(classOf[AccumulateMapper])
        job.setReducerClass(classOf[AccumulateReducer])
        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case "-avg" => {
        checkArgCount(args, 3)
        val job = JobUtil.buildJob(args, args(1).toUpperCase(), args(2).toUpperCase)
        job.setMapperClass(classOf[AverageMapper])
        job.setReducerClass(classOf[AverageReducer])
        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case "-cnt" => {
        checkArgCount(args, 2)
        val job = JobUtil.buildJob(args, args(1).toUpperCase, "none")
      }
      case _ => {
        println("Usage: [operation] [input dir] [output dir]")
        println("Run -help for assistance")
        System.exit(-1)
      }
    }
  }



  def checkArgCount(args: Array[String], minArgsReq: Int): Unit = {
    if (args.length < minArgsReq) {
      println("Too few arguments")
      println("Run -help for assistance")
      System.exit(-1)
    }
  }


  


  /** Prints help menu */
  def printHelp(): Unit = {
    println("Usage: [operation] [input dir] [output dir]")
    println("Operations:")
    println("\t-acc\t\tArtist vs popularity: returns total popularity of each artist")
    println("\t-tvd\t\tTempo vs Danceability: returns average danceability for each tempo represented")
    System.exit(0)
  }
}