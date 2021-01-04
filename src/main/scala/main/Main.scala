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
  
  if (args.length != 3) {
    println("Usage: [operation] [input dir] [output dir]")
    System.exit(-1)
  } else run(args)

  def run(args: Array[String]): Unit = {

    val job = Job.getInstance()

    job.setJarByClass(Main.getClass())
    job.setJobName("Spotify Analysis")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    args(0) match {
      case "-pop" => {
        job.setMapperClass(classOf[ArtistPopMapper])
        job.setReducerClass(classOf[ArtistPopReducer])

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])

        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case "-tvd" => {
        job.setMapperClass(classOf[TempoVsDanceMapper])
        job.setReducerClass(classOf[TempoVsDanceReducer])

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])

        val success = job.waitForCompletion(true)
        System.exit(if (success) 0 else 1)
      }
      case _ => {
        println("Usage: [operation] [input dir] [output dir]")
        System.exit(-1)
      }
    }
  }
}