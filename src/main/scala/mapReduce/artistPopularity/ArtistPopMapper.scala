package mapReduce.artistPopularity

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import song.Song


/** Mapper class for getting total popularity for each artist represented in the data set */
class ArtistPopMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  override def map(
    key: LongWritable,
    value: Text,
    context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {

    val line = value.toString()

    if (line.split('^')(Song.POPULARITY) == "popularity") return

    val artist = line.split('^')(Song.ARTISTS)
      .filter(_ != ']')
      .filter(_ != '[')
      .filter(_ != '\'')
      .toLowerCase()
      .capitalize

    val popularity = line.split('^')(Song.POPULARITY)
    println(popularity.toInt)
    context.write(new Text(artist), new IntWritable(popularity.toInt))
  }
}