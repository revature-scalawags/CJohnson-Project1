package mapReduce.average

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/** Reducer class for getting average value for each occurence of the key in the data set */
class AverageReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  
  override def reduce(
    key: Text,
    values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    
    var value = 0
    var count = 0

    values.forEach({v => 
      value += v.get()         // Sum the value for each key
      count += 1               // Count the number of times the key is represented in the dataset
    })

    var average = value / count
    context.write(key, new IntWritable(average))
  }
}

