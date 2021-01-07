package utilities

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import mapReduce.{SpotifyMapper, SpotifyReducer}


/** Provides utilities for building the MapReduce Job */
object JobUtil {


  /** Builds job with proper configuration based on user input */
  def buildJob(args: Array[String], minArgsReq: Int): Unit = {

    checkArgCount(args, minArgsReq)

    val conf = new Configuration()
    conf.set("operation", args(0).toUpperCase)
    conf.set("key", args(1).toUpperCase)
    if (minArgsReq == 3) conf.set("value", args(2).toUpperCase)

    val job = new Job(conf)

    job.setJarByClass(main.Main.getClass())
    job.setJobName("Spotify Analysis")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path(buildOutputDir(args)))

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[SpotifyMapper])
    job.setReducerClass(classOf[SpotifyReducer])

    val success = job.waitForCompletion(true)
    System.exit(if (success) 0 else 1)
  }


  /** Builds output directory based on operation of relevant fields */
  def buildOutputDir (args: Array[String]): String = {
    val outPath = new StringBuilder("output/")
    if (args(0).equalsIgnoreCase("-count")) {
      outPath.append(s"${args(0).substring(1).toLowerCase()}-")
      outPath.append(s"${args(1).substring(0,3).toLowerCase()}")
    } else {
      outPath.append(s"${args(0).substring(1)}-")
      outPath.append(s"${args(1).substring(0,3).toLowerCase()}-")
      outPath.append(s"${args(2).substring(0,3).toLowerCase()}")
    }

    outPath.toString()
  }

  
  /** Ensures the args array contains at least the minimum number of required arguments */
  def checkArgCount(args: Array[String], minArgsReq: Int): Unit = {
    if (minArgsReq == -1) {
      println("Run -help for assistance")
      System.exit(-1)
    }

    if (args.length < minArgsReq) {
      println("Too few arguments")
      println("Run -help for assistance")
      System.exit(-1)
    }
  }
}