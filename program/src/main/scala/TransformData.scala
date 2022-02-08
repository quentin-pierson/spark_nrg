import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
    val properties = new Properties()
    properties.put("user", "user")
    properties.put("password", "user")
    dataframe.write.mode(SaveMode.Append).jdbc(url,"D_TEMP", properties)
  }

  def ConvertSchema(dataframe : sql.DataFrame): sql.DataFrame ={
    var df = dataframe
    df = df.withColumnRenamed("PRODUCTION", "Code")
    df = df.withColumnRenamed("Intitulé énergie", "Intitule")
    df = df.withColumnRenamed("Unité", "Unite")
    val yearList : List[String] = List("2014", "2015", "2016", "2017", "2018", "2019")

    val schema = StructType(
      StructField("Code", StringType, true) ::
        StructField("Intitule", StringType, false) ::
        StructField("Unite", StringType, false) ::
        StructField("Value", StringType, false) ::
        StructField("Year", StringType, false) :: Nil)

    var definitive_df : DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    for(year <- yearList){
      var df_change = df.withColumn("Year", lit(year))
      val year_2 = yearList.filter(_ != year)
      df_change = df_change.withColumnRenamed(year, "Value")
      for(years <- year_2){
        df_change = df_change.drop(years)
      }
      definitive_df = definitive_df.unionAll(df_change)
    }
    definitive_df = definitive_df.sort(col("Code"),col("Year"))
    definitive_df = definitive_df.withColumn("Value",col("Value").cast(IntegerType))
    definitive_df.printSchema()
    definitive_df.show()

    definitive_df
  }

  def MainTreatment(file_name : String,
                    list_resource : List[List[String]]) = {
    val df = openDataFrame(file_name)
    val df_trans0 = df.withColumnRenamed("_c1","Intitulé énergie")

    val dataset_name = file_name.substring(4)
      .replace('_', ' ')
      .split(' ')
      .map(_.capitalize)
      .mkString(" ")

    TreatmentResources(df_trans0, dataset_name, list_resource)
  }

  def TreatmentResources(dataframe: sql.DataFrame,
                         dataset_name : String,
                         list_resource : List[List[String]]): Unit = {
    TreatmentProduction(dataframe, dataset_name, list_resource(0))
    TreatmentConsummation(dataframe, dataset_name, list_resource(1))
  }

  def TreatmentProduction(dataframe : sql.DataFrame,
                          dataset_name : String,
                          list_production : List[String]): sql.DataFrame = {
    val df = TreatmentData(dataframe, dataset_name, list_production, "PRODUCTION")

    df.coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header",true)
      .save(s"../data/refined-file/production_${dataset_name}.csv")
    df
  }

  def TreatmentConsummation(dataframe : sql.DataFrame,
                            dataset_name : String,
                            list_consomation : List[String]): sql.DataFrame = {
    val df = TreatmentData(dataframe, dataset_name, list_consomation, "CONSOMMATION")

    df.coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header",true)
      .save(s"../data/refined-file/consommation_${dataset_name}.csv")
    df
  }

  def TreatmentData(dataframe : sql.DataFrame,
                    dataset_name : String,
                    data_list: List[String],
                    resource_type : String): sql.DataFrame = {
    var df = dataframe.filter(col("PRODUCTION").isin(data_list:_*))
    df = ConvertSchema(df)
    df = AddColumn(df, dataset_name, resource_type)
    df
  }

  def AddColumn(dataframe : sql.DataFrame, Region : String, Ressource_type : String): sql.DataFrame ={
    var df = dataframe
    df = df.withColumn("Region", lit(Region))
    df = df.withColumn("Ressource_type", lit(Ressource_type))
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

  val list_resource : List[List[String]] = List(list_production, list_consomation)

  //Treatments
  MainTreatment(filesName(0), list_resource)

  for(file_name <- filesName){
    MainTreatment(file_name, list_resource)
  }

}