package utilities


/** Parses user input arguments */
object FlagParser {


  /** validates input args and builds the Job */
  def run(args: Array[String]): Unit = {

    checkArgs(args)

    // Stores the minimum number of required arguments in minArgsReq based on with operation was selected
    val minArgsReq = args(0).toLowerCase match {
      case "-sum" | "-avg" => 3
      case "-count" => 2
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
    println("Usage: [operation] [key field] [value field]")
    println("Operations:")

    println("\t-count --Returns a result set containing the number of times entries of the key field occur in the data set")
    println("\t\tExample: -count artists")

    println("")

    println("\t-sum --Returns a result set containing each entry of the key field and the total sum of values in the value field associated with that key")
    println("\t\tExample: -sum artists popularity")

    println("")

    println("\t-avg --Returns a result set containing each entry of the key field and the average of values in the value field associated with that key")
    println("\t\tExample: -avg artists popularity")

    System.exit(0)
  }
}