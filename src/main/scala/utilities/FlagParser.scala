package utilities


/** Parses user input arguments */
object FlagParser {


  /** validates input args and builds the Job */
  def run(args: Array[String]): Unit = {

    checkArgs(args)

    // Stores the minimum number of required arguments in minArgsReq based on with operation was selected
    val minArgsReq = args(0).toLowerCase match {
      case "-acc" | "-avg" => 3
      case "-cnt" => 2
      case _ => -1
    }

    JobUtil.buildJob(args, minArgsReq)
  }


  /** Checks if args array is empty or begins with '-help' */
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