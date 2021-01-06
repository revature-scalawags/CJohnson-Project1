package mapReduce.average

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import song.Song

/** Mapper class for getting average danceability for each tempo represented in the data set */
class AverageMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  
  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    
    val line = value.toString()
    val record = line.split('^')

    val conf = context.getConfiguration()
    
    if (line.split('^')(Song.TEMPO) == "tempo") return      // Skip the header line

    val outputKey = Song.formatKey(conf.get("key"), record(Song.getIndex(conf.get("key"))))

    val outputVal = Song.formatVal(conf.get("value"), record(Song.getIndex(conf.get("value"))))

    context.write(new Text(outputKey), new IntWritable(outputVal))
  }
}