package mapReduce

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/** Reducer class for getting average value for each occurence of the key in the data set */
class SpotifyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  
  override def reduce(
    key: Text,
    values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    
    val conf = context.getConfiguration()
    val operation = conf.get("operation").toUpperCase

    var value = 0
    var count = 0

    operation match {

      case "-ACC" | "-CNT" =>  {
        values.forEach(value += _.get())
        context.write(key, new IntWritable(value))
      }

      case "-AVG" => {
        values.forEach({v => 
          value += v.get()         // Sum the value for each key
          count += 1               // Count the number of times the key is represented in the dataset
        })

        var average = value / count
        context.write(key, new IntWritable(average))
      }
    }
  }
}
