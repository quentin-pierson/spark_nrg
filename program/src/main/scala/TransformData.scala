import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

import java.util.Properties

object TransformData extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "how to read csv file")
    .master(master = "local[*]")
    .getOrCreate()

  //Fonctions
  //Fonction d'ouverture de fichier
  def openDataFrame(path:String): sql.DataFrame ={
    val df = spark.read
      .option("delimiter", ",")
      .option("header","true")
      .option("inferSchema", "true")
      .csv(s"../data/raw-file/${path}.csv")
    df
  }

  def importDataToSQL(dataframe : sql.DataFrame): Unit ={
    val url = "jdbc:mysql://localhost:3306/spark_nrg?useUnicode=true&characterEncoding=utf8"
    Class.forName("com.mysql.jdbc.Driver")
    var properties = new Properties()
    properties.put("user", "user")
    properties.put("password", "user")
    dataframe.write.mode(SaveMode.Append).jdbc(url,"D_TEMP", properties)
    /*
    dataframe.write.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "D_TEMP")
      .option("user", "user")
      .option("password", "user")
      .mode(SaveMode.Append)
      .save()*/
  }

  def ConvertSchema(dataframe : sql.DataFrame): sql.DataFrame ={
    var df = dataframe
    df = df.withColumnRenamed("PRODUCTION", "Production")
    df = df.withColumnRenamed("Intitulé énergie", "intitule")
    df = df.withColumnRenamed("Unité", "Unite")
    val yearList : List[String] = List("2014", "2015", "2016", "2017", "2018", "2019")
    var i = 1
    for(year <- yearList){
      df = df.withColumn("yearTmp", df(year).cast(FloatType))
        .drop(year)
        .withColumnRenamed("yearTmp", s"annee${i}")
      i+=1
    }
    df
  }

  //variables
  //Liste des fichiers à traiter
  val filesName : List[String] =
    List("dre_auvergne_rhone_alpes",
    "dre_bourgogne_franche_comte",
    "dre_bretagne",
    "dre_centre_val_de_loire",
    "dre_corse",
    "dre_france_metropolitaine",
    "dre_grand_est",
    "dre_hauts_de_france",
    "dre_ile_de_france",
    "dre_normandie",
    "dre_nouvelle_aquitaine",
    "dre_occitanie",
    "dre_pays_de_la_loire",
    "dre_provence_alpes_cote_d_azur")

  val list_production : List[String] = List(
    "P1","P2","P3","P4","P5","P7","P8","P9","P10","P11","P12","P13","P14",
    "E1","E2","E4","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E19","E20","E21","E22","E23","E24","E25","E26","E27","E28"
  )

  val list_consomation : List[String] = List(
    "C1","CI1","CI3","CI3b","CI4","CI9","CI11","CI5","CI6","CI7","CI8",
    "CTR1","CTR2","CTR3","CTR4","CTR5","CTR6","CTR7","CTR8","CTR9","CTR10","CTR11",
    "CR1","CR7","CR2","CR3","CR4","CR5","CR6","CR8",
    "CTE1","CTE7","CTE2","CTE3","CTE4","CTE5","CTE6","CTE8",
    "CA1","CA7","CA2","CA21","CA3","CA4","CA5","CA6","CA8",
    "C2","C3","C4","C13","C14"
  )

  //Traitements
  //Enlève la limitation de l'affichage du dataframe
  spark.conf.set("org.apache.spark.sql.repl.eagerEval.enabled", true)

  //Ouverture d'un fichier .csv et affichage de dataframe
  val df = openDataFrame(filesName(0))

  //Transposition de l'affichage
  df.show(3, 0,true)

  //La deuxième colonne ne porte pas de nom .. Nous allons y remèdier maintenant 😉
  val df_trans0 = df.withColumnRenamed("_c1","Intitulé énergie")
  df_trans0.show()

  var df_production = df_trans0.filter(col("PRODUCTION").isin(list_production:_*))
  df_production = ConvertSchema(df_production)
  df_production.printSchema()
  df_production.show()

  var df_consomation = df_trans0.filter(col("PRODUCTION").isin(list_consomation:_*))
  df_consomation.show()

  val file = s"production_${filesName(0)}"
  importDataToSQL(df_production)
}