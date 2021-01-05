package mapReduce.accumulate

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import song.Song


/** Mapper class for getting total popularity for each artist represented in the data set */
class AccumulateMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    
    val line = value.toString()
    val record = line.split('^')

    val conf = context.getConfiguration()

    if (record(Song.VALENCE) == "valence") return                 // Skip the header line

    val outputKey = record(Song.getIndex(conf.get("key")))
      .filter(_ != ']')
      .filter(_ != '[')
      .filter(_ != '\'')
      .toLowerCase()
      .capitalize

    val outputVal = record(Song.getIndex(conf.get("value"))).toInt
    context.write(new Text(outputKey), new IntWritable(outputVal))
  }
}
