package mapReduce.artistPopularity

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/** Reducer class for getting total popularity for each artist represented in the data set */
class ArtistPopReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(
    key: Text,
    values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    var popularity = 0
    values.forEach(popularity += _.get())
    context.write(key, new IntWritable(popularity))
  }
}