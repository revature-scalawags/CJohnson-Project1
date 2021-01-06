package utilities

object FlagParser {


  def run(args: Array[String]): Unit = {

    checkArgs(args)

    val minArgsReq = args(0).toLowerCase match {
      case "-acc" | "-avg" => 3
      case "-cnt" => 2
      case _ => -1
    }

    JobUtil.buildJob(args, minArgsReq)
  }


  def checkArgs(args: Array[String]): Unit = {

    // Notify user that arguments must be used
    if (args.length == 0) {
      println("Arguments required. Run -help for assistance")
      System.exit(-1)
    }


    // Print help
    if (args(0) == "-help") printHelp()
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