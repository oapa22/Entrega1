import com.github.tototoshi.csv._

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import requests.Response
import scalikejdbc._
import scala.util.Try
import scala.util.{Failure, Success}
import play.api.libs.json._
import play.api.libs.json.{JsArray, JsValue, Json}
import scala.util.matching.Regex

object Scripts extends App{
  val reader = CSVReader.open(new File("C:\\Users\\user\\IdeaProjects\\Avance1\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  def escapeMysql(text: String) : String = text
    .replaceAll("\\\\", "\\\\\\\\")
    .replaceAll("\b", "\\\\b")
    .replaceAll("\n", "\\\\n")
    .replaceAll("\r", "\\\\r")
    .replaceAll("\t", "\\\\t")
    .replaceAll("\\x1A", "\\\\Z")
    .replaceAll("\\x00", "\\\\0")
    .replaceAll("'", "\\\\'")
    .replaceAll("\"", "\\\\\"")

  //---------------------------------------------------------------Director-----------------------------------------------------------------------
  /*
  case class Director(idDirector: Int,
                      name: String)

  val SQL_INSERT_PATTERN_DIRECTOR =
    """INSERT INTO Director(`idDirector`, `name`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preDirectorData = data
    .flatMap(x => x.get("director"))
    .filter(_.nonEmpty)
    .map(StringContext.processEscapes)
    .distinct
    .sorted

 println(preDirectorData.size)
  val directorDataWCC = preDirectorData.zipWithIndex.map { case (name, index) => (index, name) }
  val directorData = directorDataWCC.map { case (idDirector, name) => Director(idDirector, name) }

  val scriptDirector = directorData
    .map(director => SQL_INSERT_PATTERN_DIRECTOR.formatLocal(java.util.Locale.US,
      director.idDirector,
      escapeMysql(director.name)
    ))

  val scriptFileDirector = new File("C:\\Users\\user\\Desktop\\newSql\\director_insert.sql")
  if(scriptFileDirector.exists()) scriptFileDirector.delete()

  scriptDirector.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\director_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */


  //---------------------------------------------------------------Movie------------------------------------------------------------------------
  /*
  case class Movie(index: Int,
                   idMovie: Int,
                   budget: Long,
                   homepage: String,
                   original_language: String,
                   original_title: String,
                   overview: String,
                   popularity: Double,
                   release_date: String,
                   revenue: Long,
                   status: String,
                   runtime: Double,
                   tagline: String,
                   title: String,
                   vote_count: Double,
                   vote_average: Double,
                   director: String)

  val SQL_INSERT_PATTERN_MOVIE =
    """INSERT INTO Movie(`index`, `idMovie`, `budget`,`homepage`, `original_language`, `original_title`, `overview`, `popularity`,
      |`release_date`, `revenue`, `status`, `runtime`, `tagline`, `title`, `vote_count`, `vote_average`, `director`)
      |VALUES
      |(%d, %d, %d, '%s', '%s', '%s', '%s', %f, '%s', %d, '%s', %f, '%s', '%s', %f, %f, '%s');
      |""".stripMargin

  val movieData = data
    .map(row => Movie(
      row("index").toInt,
      row("id").toInt,
      row("budget").toLong,
      row("homepage"),
      row("original_language"),
      row("original_title"),
      row("overview"),
      row("popularity").toDouble,
      row("release_date"),
      row("revenue").toLong,
      row("status"),
      row("runtime") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0.0
        case valueOfRT => valueOfRT.toDouble
      },
      row("tagline"),
      row("title"),
      row("vote_count").toDouble,
      row("vote_average").toDouble,
      row("director") match {
        case valueDirector if valueDirector.trim.isEmpty => ""
        case valueDirector => valueDirector
      })
      )

  val scriptDataMovie = movieData
    .map(movie => SQL_INSERT_PATTERN_MOVIE.formatLocal(java.util.Locale.US,
      movie.index,
      movie.idMovie,
      movie.budget,
      movie.homepage,
      escapeMysql(movie.original_language),
      escapeMysql(movie.original_title),
      movie.overview,
      movie.popularity,
      movie.release_date,
      movie.revenue,
      movie.status,
      movie.runtime,
      movie.tagline,
      movie.title,
      movie.vote_count,
      movie.vote_average,
      escapeMysql(movie.director)
    ))

  val scriptFileMovie = new File("C:\\Users\\user\\Desktop\\newSql\\movie_insert.sql")
  if(scriptFileMovie.exists()) scriptFileMovie.delete()

  scriptDataMovie.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\movie_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )


   */
  //---------------------------------------------------------------Genre------------------------------------------------------------------------
  /*
  case class Genre(idGenre: Int,
                      name: String)

  val SQL_INSERT_PATTERN_GENRE =
    """INSERT INTO Genre(`idGenre`, `name`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preGenreData = data
    .flatMap(x => x.get("genres"))
    .filter(_.nonEmpty)
    .map(x => x.replace("Science Fiction", "Science-Fiction"))
    .map(x => x.replace("TV Movie", "TV-Movie"))
    .flatMap(x => x.split(" "))
    .distinct
    .sorted

  val genreDataWCC = preGenreData.zipWithIndex.map { case (name, index) => (index, name) }
  val genreData = genreDataWCC.map { case (idGenre, name) => Genre(idGenre, name) }

  val scriptGenre = genreData
    .map(genre => SQL_INSERT_PATTERN_GENRE.formatLocal(java.util.Locale.US,
      genre.idGenre,
      genre.name
    ))

  val scriptFileGenre = new File("C:\\Users\\user\\Desktop\\newSql\\genre_insert.sql")
  if(scriptFileGenre.exists()) scriptFileGenre.delete()

  scriptGenre.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\genre_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )


   */
  //---------------------------------------------------------------Movie-Genre------------------------------------------------------------------------
  /*
  case class MovieGenre(idMovie: Int,
                        genreName: String)

  val SQL_INSERT_PATTERN_MOVIEGENRE =
    """INSERT INTO MovieGenre(`idMovie`, `genreName`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preMovieGenreData = data
    .map(row => (row("id"), row("genres")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.replace("Science Fiction", "Science-Fiction")))
    .map(x => (x._1, x._2.replace("TV Movie", "TV-Movie")))
    .map(x => (x._1.toInt, x._2.split(" ")))
    .flatMap(x => x._2.map((x._1, _)))
    .sortBy(_._1)

  val movieGenreData = preMovieGenreData.map { case (idMovie, genreName) => MovieGenre(idMovie, genreName) }

  val scriptMovieGenre = movieGenreData
    .map(movieGenre => SQL_INSERT_PATTERN_MOVIEGENRE.formatLocal(java.util.Locale.US,
      movieGenre.idMovie,
      movieGenre.genreName
    ))

  val scriptFileMovieGenre = new File("C:\\Users\\user\\Desktop\\newSql\\movieGenre_insert.sql")
  if(scriptFileMovieGenre.exists()) scriptFileMovieGenre.delete()

  scriptMovieGenre.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\movieGenre_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */

  //---------------------------------------------------------------Movie-Keyword------------------------------------------------------------------------
  /*
  case class Keyword(idMovie: Int,
                     word: String)

  val SQL_INSERT_PATTERN_KEYWORD =
    """INSERT INTO Keyword(`idMovie`, `word`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preKeywordData = data
    .map(row => (row("id"), row("keywords")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1.toInt, x._2.split(" ")))
    .flatMap(x => x._2.map((x._1, _)))
    .sortBy(_._1)

  val keywordData = preKeywordData.map { case (idMovie, word) => Keyword(idMovie, word) }

  val scriptKeyword = keywordData
    .map(keyword => SQL_INSERT_PATTERN_KEYWORD.formatLocal(java.util.Locale.US,
      keyword.idMovie,
      escapeMysql(keyword.word)
    ))

  val scriptFileKeyword = new File("C:\\Users\\user\\Desktop\\newSql\\keyword_insert.sql")
  if(scriptFileKeyword.exists()) scriptFileKeyword.delete()

  scriptKeyword.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\keyword_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Production_Companies------------------------------------------------------------------------
  /*
  case class PCompanies(idPC: Int,
                        name: String)

  val SQL_INSERT_PATTERN_PC =
    """INSERT INTO PCompanies(`idPC`, `name`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val prePCData = data
    .flatMap(x => x.get("production_companies")).map(Json.parse)
    .flatMap(_.as[List[JsValue]])
    .map(x => (x("id").as[Int], x("name").as[String]))
    .distinct
    .sortBy(_._1)

  val PCData = prePCData.map { case (idPC, word) => PCompanies(idPC, word) }

  val scriptPC = PCData
    .map(pc => SQL_INSERT_PATTERN_PC.formatLocal(java.util.Locale.US,
      pc.idPC,
      escapeMysql(pc.name)
    ))

  val scriptFilePC = new File("C:\\Users\\user\\Desktop\\newSql\\pc_insert.sql")
  if(scriptFilePC.exists()) scriptFilePC.delete()

  scriptPC.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\pc_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Movies-PC------------------------------------------------------------------------
  /*
  case class MoviePC(idMovie: Int,
                     idPC: Int)

  val SQL_INSERT_PATTERN_MOVIEPC =
    """INSERT INTO MoviePC(`idMovie`, `idPC`)
      |VALUES
      |(%d, %d);
      |""".stripMargin

  val preMoviePCData = data
    .map(x => (x("id"), Json.parse(x("production_companies"))))
    .map(x => (x._1, x._2 \\ "id"))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[Int]))
    .sortBy(_._1)

  val moviePCData = preMoviePCData.map { case (idMoviePC, idPC) => MoviePC(idMoviePC, idPC) }

  val scriptMoviePC = moviePCData
    .map(moviepc => SQL_INSERT_PATTERN_MOVIEPC.formatLocal(java.util.Locale.US,
      moviepc.idMovie,
      moviepc.idPC
    ))

  val scriptFileMoviePC = new File("C:\\Users\\user\\Desktop\\newSql\\moviepc_insert.sql")
  if(scriptFileMoviePC.exists()) scriptFileMoviePC.delete()

  scriptMoviePC.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\moviepc_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Production_Countries------------------------------------------------------------------------
  /*
  case class PCountries(iso: String,
                        name: String)

  val SQL_INSERT_PATTERN_PCY =
    """INSERT INTO PCountries(`isoPCy`, `name`)
      |VALUES
      |('%s', '%s');
      |""".stripMargin

  val prePCyData = data
    .flatMap(x => x.get("production_countries")).map(Json.parse)
    .flatMap(_.as[List[JsValue]])
    .map(x => (x("iso_3166_1").as[String], x("name").as[String]))
    .distinct
    .sortBy(_._1)

  val PCyData = prePCyData.map { case (iso, name) => PCountries(iso, name) }

  val scriptPCy = PCyData
    .map(pcy => SQL_INSERT_PATTERN_PCY.formatLocal(java.util.Locale.US,
      pcy.iso,
      pcy.name
    ))

  val scriptFilePCy = new File("C:\\Users\\user\\Desktop\\newSql\\pcy_insert.sql")
  if(scriptFilePCy.exists()) scriptFilePCy.delete()

  scriptPCy.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\pcy_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Movies-PCy------------------------------------------------------------------------
  /*
  case class MoviePCy(idMovie: Int,
                      isoPCy: String)

  val SQL_INSERT_PATTERN_MOVIEPCY =
    """INSERT INTO MoviePCy(`idMovie`, `isoPCy`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preMoviePCyData = data
    .map(x => (x("id"), Json.parse(x("production_countries"))))
    .map(x => (x._1, x._2 \\ "iso_3166_1"))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[String]))
    .sortBy(_._1)

  val moviePCyData = preMoviePCyData.map { case (idMoviePC, isoPCy) => MoviePCy(idMoviePC, isoPCy) }

  val scriptMoviePCy = moviePCyData
    .map(moviepcy => SQL_INSERT_PATTERN_MOVIEPCY.formatLocal(java.util.Locale.US,
      moviepcy.idMovie,
      moviepcy.isoPCy
    ))

  val scriptFileMoviePCy = new File("C:\\Users\\user\\Desktop\\newSql\\moviepcy_insert.sql")
  if(scriptFileMoviePCy.exists()) scriptFileMoviePCy.delete()

  scriptMoviePCy.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\moviepcy_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Spoken_languages------------------------------------------------------------------------
  /*
  case class SpokenLanguages(iso: String,
                        name: String)

  val SQL_INSERT_PATTERN_SPOKENLAN =
    """INSERT INTO SpokenLanguages(`iso`, `name`)
      |VALUES
      |('%s', '%s');
      |""".stripMargin

  val preSpokenLanData = data
    .flatMap(x => x.get("spoken_languages")).map(Json.parse)
    .flatMap(_.as[List[JsValue]])
    .map(x => (x("iso_639_1").as[String], x("name").as[String]))
    .distinct
    .sortBy(_._1)

  val SpokenLanData = preSpokenLanData.map { case (iso, name) => SpokenLanguages(iso, name) }

  val scriptSpokenLan = SpokenLanData
    .map(spokenlan => SQL_INSERT_PATTERN_SPOKENLAN.formatLocal(java.util.Locale.US,
      spokenlan.iso,
      spokenlan.name
    ))

  val scriptFileSpokenLan = new File("C:\\Users\\user\\Desktop\\newSql\\spokenlan_insert.sql")
  if(scriptFileSpokenLan.exists()) scriptFileSpokenLan.delete()

  scriptSpokenLan.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\spokenlan_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Movies-SpokenLan------------------------------------------------------------------------
  /*
  case class MovieSL(idMovie: Int,
                      isoSL: String)

  val SQL_INSERT_PATTERN_MOVIESL =
    """INSERT INTO MovieSL(`idMovie`, `isoSL`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preMovieSLData = data
    .map(x => (x("id"), Json.parse(x("spoken_languages"))))
    .map(x => (x._1, x._2 \\ "iso_639_1"))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[String]))
    .sortBy(_._1)

  val movieSLData = preMovieSLData.map { case (idMoviePC, isoSL) => MovieSL(idMoviePC, isoSL) }

  val scriptMovieSL = movieSLData
    .map(moviesl => SQL_INSERT_PATTERN_MOVIESL.formatLocal(java.util.Locale.US,
      moviesl.idMovie,
      moviesl.isoSL
    ))

  val scriptFileMovieSL = new File("C:\\Users\\user\\Desktop\\newSql\\moviesl_insert.sql")
  if(scriptFileMovieSL.exists()) scriptFileMovieSL.delete()

    scriptMovieSL.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\moviesl_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

   */
  //---------------------------------------------------------------Crew------------------------------------------------------------------------
  /*
  case class Crew(name: String,
                  gender: Int,
                  department: String,
                  job: String,
                  credit_id: String,
                  id: Int)

  val SQL_INSERT_PATTERN_PCY =
    """INSERT INTO Crew(`name`, `gender`, `department`, `job`, `credit_id`, `id`)
      |VALUES
      |('%s', %d, '%s', '%s', '%s', %d);
      |""".stripMargin

  def replacePattern1(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "(\\s\"(.*?)\",)".r

    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern2(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern3(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "(:\\s'\"(.*?)',)".r
    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  val preCrewData = data
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern1)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(_.as[List[JsValue]])
    .map(x => (x("name").as[String], x("gender").as[Int], x("department").as[String], x("job").as[String], x("credit_id").as[String], x("id").as[Int]))
    .distinct
    .sortBy(_._5)

  val CrewData = preCrewData.map { case (name, gender, department, job, credit_id, id) => Crew(name, gender, department, job, credit_id, id) }

  val scriptCrew = CrewData
    .map(crew => SQL_INSERT_PATTERN_PCY.formatLocal(java.util.Locale.US,
      crew.name,
      crew.gender,
      crew.department,
      crew.job,
      crew.credit_id,
      crew.id
    ))

  val scriptFileCrew = new File("C:\\Users\\user\\Desktop\\newSql\\crew_insert.sql")
  if(scriptFileCrew.exists()) scriptFileCrew.delete()

  scriptCrew.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\crew_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )
  */
  //---------------------------------------------------------------Movie-Crew------------------------------------------------------------------------
  /*
  case class MovieCrew(idMovie: Int,
                       credit_id: String)

  val SQL_INSERT_PATTERN_PCY =
    """INSERT INTO MovieCrew(`idMovie`, `credit_id`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  def replacePattern1(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "(\\s\"(.*?)\",)".r

    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern2(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern3(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "(:\\s'\"(.*?)',)".r
    for(m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  val preCrewData = data
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern1)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)))
    .filter(_.isSuccess)
    .map(_.get)
    .map(x => x \\ "credit_id")
    .map(_.toList)

  val preMovieCrewData = data
    .flatMap(x => x.get("id")).zip(preCrewData)
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[String]))
    .sortBy(_._1)

  val movieCrewData = preMovieCrewData.map { case (idMovie, credit_id) => MovieCrew(idMovie, credit_id) }

  val scriptMovieCrew = movieCrewData
    .map(moviecrew => SQL_INSERT_PATTERN_PCY.formatLocal(java.util.Locale.US,
      moviecrew.idMovie,
      moviecrew.credit_id
    ))

  val scriptFileMovieCrew = new File("C:\\Users\\user\\Desktop\\newSql\\moviecrew_insert.sql")
  if(scriptFileMovieCrew.exists()) scriptFileMovieCrew.delete()

  scriptMovieCrew.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\user\\Desktop\\newSql\\moviecrew_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )
  */
  //---------------------------------------------------------------Cast------------------------------------------------------------------------










  /*
  def actorsNames(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "c191a0d702e1216f7fd851a494b87f27",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(500)
    if(response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }

  def transform(jsValue: JsValue): List[String] =
    jsValue("entity_list").as[JsArray].value
      .map(_("form"))
      .map(_.as[String])
      .toList

  val castId = data
    .map(row => (row("id"), row("cast")))
    .filter(_._2.nonEmpty)
    .map(tuple2 => (tuple2._1, StringContext.processEscapes(tuple2._2)))
    //.take(100)
    .map { t2 => (t2._1, actorsNames(t2._2)) }
    .map { t2 => (t2._1, Try(Json.parse(t2._2.get))) }
    .filter(_._2.isSuccess)
    .map(t2 => (t2._1, t2._2.get))
    .map(t2 => (t2._1, transform(t2._2)))
    .flatMap(t2 => t2._2.map(name => (t2._1, name)))
    .map(_.productIterator.toList)
    .distinct

  val f = new File("C:\\Users\\user\\Desktop\\sql\\movie_cast.csv")
  val writer = CSVWriter.open(f)
  writer.writeAll(castId)
  writer.close()
  */

}
