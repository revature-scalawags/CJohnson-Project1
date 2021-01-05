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
}
