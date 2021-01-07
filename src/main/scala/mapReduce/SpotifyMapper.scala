package mapReduce

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import song.Song


/** Mapper class for Spotify Analysis */
class SpotifyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {


  /** overrides map algorithm */
  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    
    val line = value.toString()
    val record = line.split('^')

    if (record(Song.VALENCE).equalsIgnoreCase("valence")) return       // Skip the header line

    val conf = context.getConfiguration()
    val operation = conf.get("operation")

    val outputKey = Song.formatKey(conf.get("key"), record(Song.getIndex(conf.get("key"))))

    val outputVal = operation match {
     case "-COUNT" => 1 
     case _ => Song.formatVal(conf.get("value"), record(Song.getIndex(conf.get("value"))))
    }

    context.write(new Text(outputKey), new IntWritable(outputVal))
  }
}
