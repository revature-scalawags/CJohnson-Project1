package song


/** The song object maps the name of each field in the input csv to its index (column number) */
object Song {
  val VALENCE = 0
  val YEAR = 1
  val ACOUSTICNESS = 2
  val ARTISTS = 3
  val DANCEABILITY = 4
  val DURATION = 5
  val ENERGY = 6
  val EXPLICIT = 7
  val ID = 8
  val INSTRUMENTALNESS = 9
  val KEY = 10
  val LIVENESS = 11
  val LOUDNESS = 12
  val MODE = 13
  val NAME = 14
  val POPULARITY = 15
  val RELEASE_DATE = 16
  val SPEECHINESS = 17
  val TEMPO = 18

  // keyArray contains an array of Key names
  val keyArray = Array("C", "C#/Db", "D", "D#/Eb", "E", "F", "F#/Gb", "G", "G#/Ab", "A", "A#/Bb", "B")


  /** Gets the index of the field from field name */
  def getIndex(field: String): Int = field.toUpperCase match {
    case "VALENCE" => VALENCE
    case "YEAR" => YEAR
    case "ACOUSTICNESS" => ACOUSTICNESS
    case "ARTISTS" => ARTISTS
    case "DANCEABILITY" => DANCEABILITY
    case "DURATION" => DURATION
    case "ENERGY" => ENERGY
    case "EXPLICIT" => EXPLICIT
    case "ID" => ID
    case "INSTRUMENTALNESS" => INSTRUMENTALNESS
    case "KEY" => KEY
    case "LIVENESS" => LIVENESS
    case "LOUDNESS" => LOUDNESS
    case "MODE" => MODE
    case "NAME" => NAME
    case "POPULARITY" => POPULARITY
    case "RELEASE_DATE" => RELEASE_DATE
    case "SPEECHINESS" => SPEECHINESS
    case "TEMPO" => TEMPO
    case _ => -1
  }


  /** Formats the Key data to make it easily readable */
  def formatKey(field: String, data: String): String = getIndex(field) match {
    case VALENCE | ACOUSTICNESS | DANCEABILITY |
        ENERGY | INSTRUMENTALNESS | LIVENESS |
        SPEECHINESS => (data.toDouble * 100).round.toString

    case LOUDNESS | TEMPO => data.toDouble.round.toString

    case ARTISTS => data.filter(_ != '\'').filter(_ != '[').filter(_ != ']').toLowerCase.capitalize

    case KEY => {
      if (data == -1) "No Key Data"
      else keyArray(data.toInt)
    }

    case MODE => {
      if (data.toInt == 0) "Minor"
      else if (data.toInt == 1) "Major"
      else "No Mode Data"
    }

    case EXPLICIT => {
      if (data.toInt == 0) "No"
      else if (data.toInt == 1) "Yes"
      else "No Explicit Lyric Data"
    }

    case _ => data
  }


  /** Validates and formats the Value data to make it easily readable */
  def formatVal (field: String, data: String): Int = getIndex(field) match {
    case YEAR | ARTISTS | EXPLICIT |
        ID | KEY | MODE | NAME | RELEASE_DATE => println("Value must contain numerical data"); System.exit(-1); -1
    
    case VALENCE | ACOUSTICNESS | DANCEABILITY |
        ENERGY | INSTRUMENTALNESS | LIVENESS |
        SPEECHINESS => (data.toDouble * 100).round.toInt

    case LOUDNESS | TEMPO => data.toDouble.round.toInt

    case _ => data.toInt
  }
}
