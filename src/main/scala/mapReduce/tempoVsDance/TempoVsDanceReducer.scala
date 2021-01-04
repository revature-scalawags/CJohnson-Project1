package mapReduce.tempoVsDance

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/** Reducer class for getting average danceability for each tempo represented in the data set */
class TempoVsDanceReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  
  override def reduce(
    key: Text,
    values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    
    var danceability = 0
    var count = 0

    values.forEach({d => 
      danceability += d.get()         // Sum the danceability for each tempo
      count += 1                      // Count the number of times each tempo is represented
    })

    var average = danceability / count
    context.write(key, new IntWritable(average))
  }
}

