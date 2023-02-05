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
    """INSERT INTO Director (`idDirector`, `name`)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val preDirectorData = data
    .flatMap(x => x.get("director"))
    .filter(_.nonEmpty)
    .map(StringContext.processEscapes)
    .distinct
    .sorted

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
    """INSERT INTO MOVIE (`index`, `idMovie`, `budget`,`homepage`, `original_language`, `original_title`, `overview`, `popularity`,
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
