package main

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import mapReduce.accumulate.{AccumulateMapper, AccumulateReducer}
import mapReduce.average.{AverageMapper, AverageReducer}
import org.apache.hadoop.conf.Configuration


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
    println("Usage: [operation] [key field] [value field]")
    System.exit(-1)
  } else run(args)


  /** Parses the arguments provided and runs the proper MapReduce algorithm */
  def run(args: Array[String]): Unit = {

    args(0) match {
      case "-acc" => {
        val job = buildJob(args(1).toUpperCase(), args(2).toUpperCase())
        job.setMapperClass(classOf[AccumulateMapper])
        job.setReducerClass(classOf[AccumulateReducer])
        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case "-avg" => {
        val job = buildJob(args(1).toUpperCase(), args(2).toUpperCase())
        job.setMapperClass(classOf[AverageMapper])
        job.setReducerClass(classOf[AverageReducer])
        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case _ => {
        println("Usage: [operation] [input dir] [output dir]")
        println("Run -help for assistance")
        System.exit(-1)
      }
    }
  }


  def buildJob(key: String, value: String): Job = {
    val conf = new Configuration()
    conf.set("key", key)
    conf.set("value", value)

    val job = new Job(conf)

    job.setJarByClass(Main.getClass())
    job.setJobName("Spotify Analysis")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output"))

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
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