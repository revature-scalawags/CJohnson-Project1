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
    var count = 0

    values.forEach({d => 
      danceability += d.get()
      count += 1
    })

    var average = danceability / count
    context.write(key, new IntWritable(average))
  }
}

