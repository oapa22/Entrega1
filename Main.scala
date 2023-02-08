package Avance1

import com.github.tototoshi.csv._
import java.io.File

import play.api.libs.json._
import play.api.libs.json.{JsArray, JsValue, Json}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.{BarChart, Histogram}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultElements, DefaultTheme, defaultTheme}
import scala.util.matching.Regex
import scala.util.Try
import scala.util.{Failure, Success}
object Main extends App{

  val reader = CSVReader.open(new File(
    "C:\\Users\\user\\IdeaProjects\\Avance1\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //Función para calcular el promedio
  val prom = (valores : List[Double]) => valores.sum.toDouble/valores.length

  //Lista con los títulos de las películas
  val titulo = data.flatMap(x => x.get("original_title"))

  //Funcion para realizar graficos con valores muy distintos
  def cbrt(x: Double): Double = java.lang.Math.cbrt(x)

  //Para que los nombres de las gráficas no se sobrepongan
  implicit val theme = DefaultTheme.copy(
    elements = DefaultElements.copy(categoricalXAxisLabelOrientation = 45)
  )
  // BUDGET
  println("Columna budget")
  //MAYOR
  val mayorBudget = data.flatMap(x => x.get("budget")).map(_.toDouble).max
  println("Mayor: "+mayorBudget)
  //MENOR
  val menorBudget = data.flatMap(x => x.get("budget")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorBudget)
  //PROMEDIO
  val promconCerosBudget = data.flatMap(x => x.get("budget")).map(_.toDouble)
  val promsinCerosBudget = data.flatMap(x => x.get("budget")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosBudget))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosBudget))
  println("")
  //Gráfico
  val preBudget = data.flatMap(x => x.get("budget")).map(_.toDouble)
  val datosBudget = titulo.zip(preBudget)
  val budget = datosBudget.groupBy {
    case (nombre, ganancias) => (nombre, ganancias)
  }.map {
    case (nombre, ganancias) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val budgetValues = budget.take(10).map(_._2).map(_.toDouble)
  val budgetLabels = budget.take(10).map(_._1)
  BarChart(budgetValues)(theme)
    .title("Películas con mayor presupuesto")(theme)
    .xAxis(budgetLabels)(theme)
    .yAxis()(theme)
    .ybounds(225000000)
    .frame()(theme)
    .yLabel("Presupuesto en $")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\budgetMay.png"))
  val budgetValues2 = budget.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val budgetLabels2 = budget.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(budgetValues2)(theme)
    .title("Películas con menor presupuesto")(theme)
    .xAxis(budgetLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .ybounds(0.0, 15.0)
    .yLabel("Presupuesto en $")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\budgetMen.png"))

  // POPULARITY
  println("Columna popularity")
  //MAYOR
  val mayorPopularity = data.flatMap(x => x.get("popularity")).map(_.toDouble).max
  println("Mayor: "+mayorPopularity)
  //MENOR
  val menorPopularity = data.flatMap(x => x.get("popularity")).map(_.toDouble).min
  println("Menor: "+menorPopularity)
  //PROMEDIO
  val promconCerosPopularity = data.flatMap(x => x.get("popularity")).map(_.toDouble)
  val promsinCerosPopularity = data.flatMap(x => x.get("popularity")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosPopularity))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosPopularity))
  println("")
  //Gráfico
  val prePopularity = data.flatMap(x => x.get("popularity")).map(_.toDouble)
  val datosPopularity = titulo.zip(prePopularity)
  val popularity = datosPopularity.groupBy {
    case (nombre, pupularidad) => (nombre, pupularidad)
  }.map {
    case (nombre, pupularidad) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val popularityValues = popularity.take(10).map(_._2).map(_.toDouble)
  val popularityLabels = popularity.take(10).map(_._1)
  BarChart(popularityValues)(theme)
    .title("Películas con mayor popularidad")(theme)
    .xAxis(popularityLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Popularidad")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\popularityMay.png"))
  val popularityValues2 = popularity.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val popularityLabels2 = popularity.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(popularityValues2)(theme)
    .title("Películas con menor popularidad")(theme)
    .xAxis(popularityLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Popularidad")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\popularityMen.png"))

  // REVENUE
  println("Columna revenue")
  //MAYOR
  val mayorRevenue = data.flatMap(x => x.get("revenue")).map(_.toDouble).max
  println("Mayor: "+mayorRevenue)
  //MENOR
  val menorRevenue = data.flatMap(x => x.get("revenue")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorRevenue)
  //PROMEDIO
  val promconCerosRevenue = data.flatMap(x => x.get("revenue")).map(_.toDouble)
  val promsinCerosRevenue = data.flatMap(x => x.get("revenue")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosRevenue))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosRevenue))
  println("")
  //Gráfico
  val preRevenue = data.flatMap(x => x.get("revenue")).map(_.toDouble)
  val datosRevenue = titulo.zip(preRevenue)
  val revenue = datosRevenue.groupBy {
    case (nombre, ganancias) => (nombre, ganancias)
  }.map {
    case (nombre, ganancias) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val revenueValues = revenue.take(10).map(_._2).map(_.toDouble)
  val revenueLabels = revenue.take(10).map(_._1)
  BarChart(revenueValues)(theme)
    .title("Películas con mayores ganancias")(theme)
    .xAxis(revenueLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Ganancias en $")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\revenueMay.png"))
  val revenueValues2 = revenue.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val revenueLabels2 = revenue.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(revenueValues2)(theme)
    .title("Películas con menores ganancias")(theme)
    .xAxis(revenueLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Ganancias en $")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\revenueMen.png"))

  // RUNTIME
  println("Columna runtime")
  //MAYOR
  val mayorRuntime = data.flatMap(x => x.get("runtime")).filter(x => x != "").map(_.toDouble).max
  println("Mayor: "+mayorRuntime)
  //MENOR
  val menorRuntime = data.flatMap(x => x.get("runtime")).filter(x => x != "").map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorRuntime)
  //PROMEDIO
  val promconCerosRuntime = data.flatMap(x => x.get("runtime")).filter(x => x != "").map(_.toDouble)
  val promsinCerosRuntime = data.flatMap(x => x.get("runtime")).filter(x => x != "").map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosRuntime))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosRuntime))
  println("")
  //Gráfico
  val preRuntime = data.flatMap(x => x.get("runtime")).filter(x => x != "").map(_.toDouble)
  val datosRuntime = titulo.zip(preRuntime)
  val runtime = datosRuntime.groupBy {
    case (nombre, duracion) => (nombre, duracion)
  }.map {
    case (nombre, duracion) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val runtimeValues = runtime.take(10).map(_._2).map(_.toDouble)
  val runtimeLabels = runtime.take(10).map(_._1)
  BarChart(runtimeValues)(theme)
    .title("Películas con mayor duración")(theme)
    .xAxis(runtimeLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Duración en minutos")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\runtimeMay.png"))
  val runtimeValues2 = runtime.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val runtimeLabels2 = runtime.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(runtimeValues2)(theme)
    .title("Películas con menor duración")(theme)
    .xAxis(runtimeLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Duración en minutos")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\runtimeMen.png"))

  // VOTE_AVERAGE
  println("Columna vote_average")
  //MAYOR
  val mayorVoteA = data.flatMap(x => x.get("vote_average")).map(_.toDouble).max
  println("Mayor: "+mayorVoteA)
  //MENOR
  val menorVoteA = data.flatMap(x => x.get("vote_average")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorVoteA)
  //PROMEDIO
  val promconCerosVoteA = data.flatMap(x => x.get("vote_average")).map(_.toDouble)
  val promsinCerosVoteA = data.flatMap(x => x.get("vote_average")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosVoteA))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosVoteA))
  println("")
  //Gráfico
  val calificacion = data.flatMap(x => x.get("vote_average")).map(_.toDouble)
  val datosVotosA = titulo.zip(calificacion)
  val voteAvg = datosVotosA.groupBy {
    case (nombre, votos) => (nombre, votos)
  }.map {
    case (nombre, votos) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val voteAValues = voteAvg.take(10).map(_._2).map(_.toDouble)
  val voteALabels = voteAvg.take(10).map(_._1)
  BarChart(voteAValues)(theme)
    .title("Películas con mayor puntaje")(theme)
    .xAxis(voteALabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Puntaje")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\voteAvgMay.png"))
  val voteAValues2 = voteAvg.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val voteALabels2 = voteAvg.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(voteAValues2)(theme)
    .title("Películas con menor puntaje")(theme)
    .xAxis(voteALabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Puntaje")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\voteAvgMen.png"))

  // VOTE_COUNT
  println("Columna vote_count")
  //MAYOR
  val mayorVoteC = data.flatMap(x => x.get("vote_count")).map(_.toDouble).max
  println("Mayor: "+mayorVoteC)
  //MENOR
  val menorVoteC = data.flatMap(x => x.get("vote_count")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorVoteC)
  //PROMEDIO
  val promconCerosVoteC = data.flatMap(x => x.get("vote_count")).map(_.toDouble)
  val promsinCerosVoteC = data.flatMap(x => x.get("vote_count")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosVoteC))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosVoteC))
  println("")
  //Gráfico
  val votos = data.flatMap(x => x.get("vote_count")).map(_.toDouble)
  val datosVotosC = titulo.zip(votos)
  val voteCount = datosVotosC.groupBy {
    case (nombre, votos) => (nombre, votos)
  }.map {
    case (nombre, votos) => (nombre._1, nombre._2)
  }.toList.sortBy(_._2).reverse
  val voteCValues = voteCount.take(10).map(_._2).map(_.toDouble)
  val voteCLabels = voteCount.take(10).map(_._1)
  BarChart(voteCValues)(theme)
    .title("Películas con mayor cantidad de votos")(theme)
    .xAxis(voteCLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Número de votos")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\voteCountMay.png"))
  val voteCValues2 = voteCount.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val voteCLabels2 = voteCount.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(voteCValues2)(theme)
    .title("Películas con menor cantidad de votos")(theme)
    .xAxis(voteCLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Número de votos")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\voteCountMen.png"))

  // RELEASE_DATE
  println("Columna release_date")
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDateList = data
    .map(row => row("release_date"))
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))
  //val yearReleaseList = releaseDateList.map(_.getYear)
  val yearReleaseList = releaseDateList
    .map(_.getYear)
    .map(_.toDouble)
  printf("Año menor en el que se ha lanzado una película: %f\n", yearReleaseList.min)
  printf("Año mayor en el que se ha lanzado una película: %f\n", yearReleaseList.max)
  printf("Promedio de años en los que se ha lanzado una película: %f\n", prom(yearReleaseList))
  //Gráfico
  Histogram(yearReleaseList)(defaultTheme)
    .title("Años de lanzamiento")(defaultTheme)
    .xAxis()(defaultTheme)
    .yAxis()(defaultTheme)
    .xbounds(1916.0, 2018.0)
    .render()(defaultTheme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\releaseDates.png"))

  //Distribucion de frecuencia

  //Géneros
  val genres = data
    .flatMap(x => x.get("genres"))
    .filter(_.nonEmpty)
    .map(x => x.replace("Science Fiction", "Science-Fiction"))
    .map(x => x.replace("TV Movie", "TV-Movie"))
    .flatMap(x => x.split(" "))
  val generosAparicion = genres.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nLista de géneros y en cuantas películas han aparecido: " + generosAparicion)
  println("Género con menos aparición: " + generosAparicion.minBy(_._2))
  println("Género con más aparición: " + generosAparicion.maxBy(_._2))
  //Gráfico
  val generosValues = generosAparicion.take(10).map(_._2).map(_.toDouble)
  val generosLabels = generosAparicion.take(10).map(_._1)
  BarChart(generosValues)(theme)
    .title("Géneros con menor aparición")(theme)
    .xAxis(generosLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\generosMen.png"))
  val generosValues2 = generosAparicion.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val generosLabels2 = generosAparicion.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(generosValues2)(theme)
    .title("Géneros con mayor aparición")(theme)
    .xAxis(generosLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\generosMay.png"))

  //Idiomas
  val original_language = data.flatMap(x => x.get("original_language"))
  val idiomas = original_language.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nLista de los idiomas y su cantidad: " + idiomas)
  println("Idioma menos usados: " + idiomas.minBy(_._2))
  println("Idioma más usados: " + idiomas.maxBy(_._2))
  //Gráfico
  val idiomasValues = idiomas.take(10).map(_._2).map(_.toDouble)
  val idiomasLabels = idiomas.take(10).map(_._1)
  BarChart(idiomasValues)(theme)
    .title("Idiomas con menor aparición")(theme)
    .xAxis(idiomasLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\idiomasMen.png"))
  val idiomasValues2 = idiomas.reverse.filter(x => x._2 != 0).take(10).map(_._2).map(_.toDouble)
  val idiomasLabels2 = idiomas.reverse.filter(x => x._2 != 0).take(10).map(_._1)
  BarChart(idiomasValues2)(theme)
    .title("Idiomas con mayor aparición")(theme)
    .xAxis(idiomasLabels2)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .ybounds(1.0, 100.0)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\idiomasMay.png"))

  //Estado
  val status = data.flatMap(x => x.get("status"))
  val estados = status.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nLista de los distintos tipos de estado: " + estados)
  println("Estado menos usado: " + estados.minBy(_._2))
  println("Estado más usado: " + estados.maxBy(_._2))
  //Gráfico
  val estadosValues = estados.take(10).map(_._2).map(_.toDouble)
  val estadosLabels = estados.take(10).map(_._1)
  val estados2 = estadosLabels.zip(estadosValues.map(x => cbrt(x)))
  PieChart(estados2)(theme)
    .title("Estados de películas")(theme)
    .rightLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\test\\estados3.png"))

  //Actores
  val cast = data.flatMap(x => x.get("cast")).flatMap(x => x.split(" ").toList)
  val actores = cast.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  //println("Lista de los actores: " + actores)
  //println("Actor con menos aparción en películas: " + actores.minBy(_._2))
  println("\nColumna actores: ")
  println("Nombre más común entre los actores: " + actores.maxBy(_._2))
  //Gráfico
  val actoresValues = actores.reverse.take(10).map(_._2).map(_.toDouble)
  val actoresLabels = actores.reverse.take(10).map(_._1)
  BarChart(actoresValues)(theme)
    .title("Nombres más común entre los actores")(theme)
    .xAxis(actoresLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\actores.png"))

  //Director
  val director = data.flatMap(x => x.get("director")).filter(x => x != "")
  val directorAparicion = director.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
    print("\nDirector que ha dirigido mas películas: ")
    println(directorAparicion.maxBy(_._2))
  println("")
  //Gráfico
  val directorValues = directorAparicion.reverse.take(10).map(_._2).map(_.toDouble)
  val directorLabels = directorAparicion.reverse.take(10).map(_._1)
  BarChart(directorValues)(theme)
    .title("Director que más películas ha dirigido")(theme)
    .xAxis(directorLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\directores.png"))

  //JSON
  val jsonEjemplo: JsValue = Json.parse("""
  {
    "name" : "Watership Down",
    "location" : {
      "lat" : 51.235685,
      "long" : -1.309197
    },
    "residents" : [ {
      "name" : "Fiver",
      "age" : 4,
      "role" : null
    }, {
      "name" : "Bigwig",
      "age" : 6,
      "role" : "Owsla"
    } ]
  }
  """)

  println("Ejemplo de uso de Json: ")
  val lat = (jsonEjemplo \ "location" \ "lat").get

  println(Json.stringify(jsonEjemplo))

  val bigwig2 = (jsonEjemplo \ "residents" \ 1).get
  println(bigwig2)

  val names = jsonEjemplo \\ "name"
  println(names)

  val name = (jsonEjemplo \ "name").as[String]

  //Consultas dentro del csv
  println("\nConsultas con los datos del csv: ")
  //JSON spoken_language
  val jsonSpokenLanguage = data.flatMap(x => x.get("spoken_languages")).map(Json.parse)
  val idiomas2 = jsonSpokenLanguage.flatMap(_ \\ "name").groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nLista de los distintos tipos de idiomas: " + idiomas2)
  println("Idioma menos usado: " + idiomas2.minBy(_._2))
  println("Idioma más usado: " + idiomas2.maxBy(_._2))
  //Gráfico
  val spokenLanguages = data
    .flatMap(row => row.get("spoken_languages"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map{ case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse
  val languagesValues = spokenLanguages.take(10).map(_._2).map(_.toDouble)
  val languagesLabels = spokenLanguages.take(10).map(_._1)
  BarChart(languagesValues)(theme)
    .title("Idiomas hablados")(theme)
    .xAxis(languagesLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\languages.png"))

  //JSON production_companies
  val jsonProductionCompanies = data.flatMap(x => x.get("production_companies")).map(Json.parse)
  val companias = jsonProductionCompanies.flatMap(_ \\ "name").groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  //println("\nLista de las compañías con mas aportaciones en películas: " + companias)
  println("\nConsultas sobre las compañías: ")
  println("Compañía con menor aporte: " + companias.minBy(_._2))
  println("Compañía con mayor aporte: " + companias.maxBy(_._2))
  //Gráfico
  val productionCompanies = data
    .flatMap(row => row.get("production_companies"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map{ case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse
  val pCompaniesValues = productionCompanies.take(10).map(_._2).map(_.toDouble)
  val pCompanieLabels = productionCompanies.take(10).map(_._1)
  BarChart(pCompaniesValues)(theme)
    .title("Compañías productoras")(theme)
    .xAxis(pCompanieLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\pCompanies.png"))

  //JSON production_countries
  val jsonProductionCountries = data.flatMap(x => x.get("production_countries")).map(Json.parse)
  val paises = jsonProductionCountries.flatMap(_ \\ "name").groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nLista de las países con aparación en películas: " + paises)
  println("País con menor aparición en películas: " + paises.minBy(_._2))
  println("País con mayor aparición en películas: " + paises.maxBy(_._2))
  //Gráfico
  val productionCountries = data
    .flatMap(row => row.get("production_countries"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map{ case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse
  val pCountriesValues = productionCountries.take(10).map(_._2).map(_.toDouble)
  val pCountriesLabels = productionCountries.take(10).map(_._1)
  BarChart(pCountriesValues)(theme)
    .title("Países productores")(theme)
    .xAxis(pCountriesLabels)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Producciones")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\pCountries.png"))

  //JSON crew
  def replacePattern(original : String) : String = {
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

  //JSON crew
  val crew = data
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Json.parse(text))

  val departamentos = crew.map(_ \\ "department")
  val trabajos = crew.map(_ \\ "job")
  val datos1 = titulo.zip(departamentos)
  val datos2 = titulo.zip(trabajos)
  println("\nDepartamentos de la película \"Signed, Sealed, Delivered\": ")
  println(datos1.filter(x => x._1.contains("Signed, Sealed, Delivered")).flatMap(x => x._2).distinct)
  val listaCom = crew.flatMap(_ \\ "department").distinct
  println("Departamenos de todas las peliculas: "+ listaCom)
  println("")
  println("Trabajos de la película \"Shanghai Calling\": ")
  println(datos2.filter(x => x._1.contains("Shanghai Calling")).flatMap(x => x._2).distinct)
  val listaCom2 = crew.flatMap(_ \\ "job").distinct
  println("Trabajos de todas las peliculas: "+ listaCom2)
  val departamentosCrew = datos1.flatMap(x => x._2).groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nDepartamentos menos populares: " + departamentosCrew.minBy(_._2))
  println("Departamentos más populares: " + departamentosCrew.maxBy(_._2))
  //Gráfico
  val barrasDepartamentos = crew
    .flatMap(_ \\ "department")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map{ case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse
  val bDepValores = barrasDepartamentos.take(5).map(_._2).map(_.toDouble)
  val bDepNombres = barrasDepartamentos.take(5).map(_._1)
  BarChart(bDepValores)(theme)
    .title("Departamentos más populares")(theme)
    .xAxis(bDepNombres)(theme)
    .yAxis()(theme)
    .frame()(theme)
    .yLabel("Personas que trabajan en el departamento")(theme)
    .bottomLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\departamentos.png"))

  val crewG = crew.flatMap(_ \\ "gender").map(x => x.toString)
  val crewGender = crewG.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)

  println(crewGender)
  //Gráfico
  val genderValues = crewGender.map(_._2).map(_.toDouble)
  val genderLabels = crewGender.map(_._1)
  val gender2 = genderLabels.zip(genderValues.map(x => cbrt(x)))
  PieChart(gender2)(theme)
    .title("Géneros entre miembros de crew")(theme)
    .rightLegend()(theme)
    .render()(theme)
    .write(new File("C:\\Users\\user\\Desktop\\histogramas\\test\\generos3.png"))

}
