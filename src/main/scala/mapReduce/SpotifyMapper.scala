package mapReduce

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import song.Song


/** Mapper class for getting total popularity for each artist represented in the data set */
class SpotifyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {


  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    
    val line = value.toString()
    val record = line.split('^')

    val conf = context.getConfiguration()

    val operation = conf.get("operation")

    if (record(Song.VALENCE) == "valence") return       // Skip the header line

    val outputKey = Song.formatKey(conf.get("key"), record(Song.getIndex(conf.get("key"))))

    val outputVal = operation match {
     case "-CNT" => 1 
     case _ => Song.formatVal(conf.get("value"), record(Song.getIndex(conf.get("value"))))
    }

    context.write(new Text(outputKey), new IntWritable(outputVal))
  }
}
