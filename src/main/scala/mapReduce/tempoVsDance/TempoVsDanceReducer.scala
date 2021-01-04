package mapReduce.tempoVsDance

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.ArrayWritable

class TempoVsDanceReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  
  override def reduce(
    key: Text,
    values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    
    var danceability = 0

    values.forEach(danceability += _.get())
    context.write(key, new IntWritable(danceability))
  }
}

