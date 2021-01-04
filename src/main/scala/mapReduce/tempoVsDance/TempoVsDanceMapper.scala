package mapReduce.tempoVsDance

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import song.Song

/** Mapper class for getting average danceability for each tempo represented in the data set */
class TempoVsDanceMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  
  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    val line = value.toString()
    
    if (line.split('^')(Song.TEMPO) == "tempo") return      // Skip the header line

    val tempo = line.split('^')(Song.TEMPO).toDouble.round.toInt.toString()
    val dance = (line.split('^')(Song.DANCEABILITY).toDouble * 100).round.toInt

    context.write(new Text(tempo), new IntWritable(dance))
  }
}