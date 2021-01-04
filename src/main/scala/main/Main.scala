package main

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import mapReduce.artistPopularity.{ArtistPopMapper, ArtistPopReducer}
import mapReduce.tempoVsDance.{TempoVsDanceMapper, TempoVsDanceReducer}


/** Application using Scala and Hadoop to process Spotify song data */
object Main extends App {
  
  // Notify user that arguments must be used
  if (args.length == 0) {
    println("Arguments required run -help for assistance")
    System.exit(-1)
  }
  
  // Print help
  if (args(0) == "-help") printHelp()

  // If number of arguments is correct, run the selected operation
  if (args.length != 3) {
    println("Usage: [operation] [input dir] [output dir]")
    System.exit(-1)
  } else run(args)


  /** Parses the arguments provided and runs the proper MapReduce algorithm */
  def run(args: Array[String]): Unit = {
    val job = Job.getInstance()

    job.setJarByClass(Main.getClass())
    job.setJobName("Spotify Analysis")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    args(0) match {
      case "-pop" => {
        job.setMapperClass(classOf[ArtistPopMapper])
        job.setReducerClass(classOf[ArtistPopReducer])
      }
      case "-tvd" => {
        job.setMapperClass(classOf[TempoVsDanceMapper])
        job.setReducerClass(classOf[TempoVsDanceReducer])
      }
      case _ => {
        println("Usage: [operation] [input dir] [output dir]")
        println("Run -help for assistance")
        System.exit(-1)
      }
    }

    val success = job.waitForCompletion(true)
    System.exit(if (success) 0 else 1)
  }

  
  /** Prints help menu */
  def printHelp(): Unit = {
    println("Usage: [operation] [input dir] [output dir]")
    println("Operations:")
    println("\t-avp\t\tArtist vs popularity: returns total popularity of each artist")
    println("\t-tvd\t\tTempo vs Danceability: returns average danceability for each tempo represented")
    System.exit(0)
  }
}