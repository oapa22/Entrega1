package Avance1

import com.github.tototoshi.csv._
import play.api.libs.json._
import play.api.libs.json.{JsArray, JsValue, Json}

import java.io.File

object Main extends App{
  val reader = CSVReader.open(new File("C:\\Users\\user\\IdeaProjects\\Avance1\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //funcion para promedio
  val prom = (valores : List[Double]) => valores.sum.toDouble/valores.length

  // INDEX
  println("Columna index")
  //MAYOR
  val mayorIndex = data.flatMap(x => x.get("index")).map(_.toDouble).max
  println("Mayor: "+mayorIndex)
  //MENOR
  val menorIndex = data.flatMap(x => x.get("index")).map(_.toDouble).min
  println("Menor: "+menorIndex)
  //PROMEDIO
  val promconCerosIndex = data.flatMap(x => x.get("index")).map(_.toDouble)
  val promsinCerosIndex = data.flatMap(x => x.get("index")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosIndex))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosIndex))
  println("")

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

  // ID
  println("Columna id")
  //MAYOR
  val mayorId = data.flatMap(x => x.get("id")).map(_.toDouble).max
  println("Mayor: "+mayorId)
  //MENOR
  val menorId = data.flatMap(x => x.get("id")).map(_.toDouble).min
  println("Menor: "+menorId)
  //PROMEDIO
  val promId = data.flatMap(x => x.get("id")).map(_.toDouble)
  print("Promedio: ")
  println(prom(promId))
  println("")

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

  // RUNTIME
  println("Columna runtime")
  //MAYOR
  val mayorRuntime = data.flatMap(x => x.get("runtime")).filter(x => x.contains(".")).map(_.toDouble).max
  println("Mayor: "+mayorRuntime)
  //MENOR
  val menorRuntime = data.flatMap(x => x.get("runtime")).filter(x => x.contains(".")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorRuntime)
  //PROMEDIO
  val promconCerosRuntime = data.flatMap(x => x.get("runtime")).filter(x => x.contains(".")).map(_.toDouble)
  val promsinCerosRuntime = data.flatMap(x => x.get("runtime")).filter(x => x.contains(".")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosRuntime))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosRuntime))
  println("")


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

  //Distribucion de frecuencia
  //Generos
  val genres = data.flatMap(x => x.get("genres")).flatMap(x => x.split(" ").toList)
  val generosAparicion = genres.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("Lista de géneros y en cuantas películas han aparecido: " + generosAparicion)
  println("Género con menos aparición: " + generosAparicion.minBy(_._2))
  println("Género con más aparición: " + generosAparicion.maxBy(_._2))

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
  println("Nombre más usado entre los actores: " + actores.maxBy(_._2))

  //Director
  val director = data.flatMap(x => x.get("director"))
  val directorAparicion = director.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2).maxBy(_._2)
    print("\nDirector que ha dirigido mas películas: ")
    println(directorAparicion)
  println("")
  //En este caso el que mas apariciones ha tenido es el campo vacio

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
  println("\nLista de los distintos tipos de idioma: " + idiomas2)
  println("Idioma menos usado: " + idiomas2.minBy(_._2))
  println("Idioma más usado: " + idiomas2.maxBy(_._2))

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

  /*
  //JSON crew
  val jsonCrew = data.flatMap(x => x.get("crew")).map(Json.parse)
  val crew = jsonCrew.flatMap(_ \\ "name").groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2)
  println("\nColumna crew: ")
  println("Persona que menos ha participado dentro del crew: " + crew.minBy(_._2))
  println("Persona que más ha participado dentro del crew: " + crew.maxBy(_._2))
   */

}
