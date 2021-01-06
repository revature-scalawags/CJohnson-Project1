package utilities

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object JobUtil {

  def buildJob(args: Array[String], key: String, value: String): Job = {
    val conf = new Configuration()
    conf.set("key", key)
    conf.set("value", value)

    val job = new Job(conf)

    job.setJarByClass(main.Main.getClass())
    job.setJobName("Spotify Analysis")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path(buildOutputDir(args)))

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job
  }


  def buildOutputDir (args: Array[String]): String = {
    val outPath = new StringBuilder("output/")
    if (args(0).equalsIgnoreCase("-cnt")) {
      outPath.append(s"${args(0).substring(1).toLowerCase()}-")
      outPath.append(s"${args(1).substring(0,3).toLowerCase()}")
    } else {
      outPath.append(s"${args(0).substring(1)}-")
      outPath.append(s"${args(1).substring(0,3).toLowerCase()}-")
      outPath.append(s"${args(2).substring(0,3).toLowerCase()}")
    }

    outPath.toString()
  }
}