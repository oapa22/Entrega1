package Avance1

import com.github.tototoshi.csv._
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

  /*
  // RUNTIME
  println("Columna runtime")
  //MAYOR
  val mayorRuntime = data.flatMap(x => x.get("runtime")).map(_.toDouble).max
  println("Mayor: "+mayorRuntime)
  //MENOR
  val menorRuntime = data.flatMap(x => x.get("runtime")).map(_.toDouble).filter(x => x != 0).min
  println("Menor: "+menorRuntime)
  //PROMEDIO
  val promconCerosRuntime = data.flatMap(x => x.get("runtime")).map(_.toDouble)
  val promsinCerosRuntime = data.flatMap(x => x.get("runtime")).map(_.toDouble).filter(x => x != 0)
  print("Promedio con ceros: ")
  println(prom(promconCerosRuntime))
  print("Promedio sin ceros: ")
  println(prom(promsinCerosRuntime))
  println("")

   */

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
  println("Idiomas menos usados: " + idiomas.minBy(_._2))
  println("Idiomas más usados: " + idiomas.maxBy(_._2))

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
  println("Nombre más usado entre los actores: " + actores.maxBy(_._2))

  //Director
  val director = data.flatMap(x => x.get("director"))
  val directorAparicion = director.groupBy {
    case nombre => nombre
  }.map {
    case nombre => (nombre._1, nombre._2.size)
  }.toList.sortBy(_._2).maxBy(_._2)
    print("\n\nDirector que ha dirigido mas películas: ")
    println(directorAparicion)
  //En este caso el que mas apariciones ha tenido es el campo nulo


}
