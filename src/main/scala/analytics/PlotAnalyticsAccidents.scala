package fr.addinn.analytics

import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import javafx.scene.{chart => jfxsc}
import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.chart._
import scalafx.scene.layout.StackPane



object PlotAnalyticsAccidents extends JFXApp {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder.master("local[*]").appName("transport").getOrCreate()

  val Meteotransport = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("meteotran.csv")
  import spark.implicits._

  var hommeValueJanvier = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 1)).count()
  var femmeValueJanvier = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 1)).count()

  var hommeValueFevrier = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 2)).count()
  var femmeValueFevrier = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 2)).count()

  var hommeValueMars = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 3)).count()
  var femmeValueMars = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 3)).count()

  var hommeValueAvril = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 4)).count()
  var femmeValueAvril = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 4)).count()

  var hommeValueMai = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 5)).count()
  var femmeValueMai = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 5)).count()

  var hommeValueJuin = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 6)).count()
  var femmeValueJuin = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 6)).count()


  var hommeValueJuillet = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 7)).count()
  var femmeValueJuillet = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 7)).count()

  var hommeValueAout = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 8)).count()
  var femmeValueAout = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 8)).count()

  var hommeValueSeptembre = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 9)).count()
  var femmeValueSeptembre = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 9)).count()

  var hommeValueOctobre = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 10)).count()
  var femmeValueOctobre = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 10)).count()

  var hommeValueNovembre = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 11)).count()
  var femmeValueNovembre = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 11)).count()

  var hommeValueDecembre = Meteotransport.select("*").filter($"sexe" === 1 and ($"mois" === 12)).count()
  var femmeValueDecembre = Meteotransport.select("*").filter($"sexe" === 2 and ($"mois" === 12)).count()





      val xAxis = new CategoryAxis()
      val yAxis = new NumberAxis()
      val barChart = BarChart(xAxis, yAxis)
      barChart.title = "Nombre d'accident 2017"
      barChart.data = createChartData()
      barChart.barGap = 1

      stage = new PrimaryStage {
        title = "Ville bordeaux"
        scene = new Scene(900, 500) {
          root = new StackPane {
            children = barChart
          }
        }
      }


      private def createChartData(): ObservableBuffer[jfxsc.XYChart.Series[String, Number]] = {




        val answer = new ObservableBuffer[jfxsc.XYChart.Series[String, Number]]()
        val homme = new XYChart.Series[String, Number] {
          name = "Homme"
        }
        val femme = new XYChart.Series[String, Number] {
          name = "Femme"
        }

        for (i <- 1 to 12) {

            if ( i ==1){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueJanvier)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueJanvier)
            }else if (i==2){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueFevrier)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueFevrier)
            }else if (i==3){
            homme.data() += XYChart.Data[String, Number](i.toString, hommeValueMars)
            femme.data() += XYChart.Data[String, Number](i.toString, femmeValueMars)
          }else if (i==4){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueAvril)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueAvril)
            }else if (i==5){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueMai)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueMai)
            }else if (i==6){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueJuin)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueJuin)
            }else if (i==7){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueJuillet)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueJuillet)
            }else if (i==8){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueAout)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueAout)
            }else if (i==9){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueSeptembre)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueSeptembre)
            }else if (i==10){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueOctobre)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueOctobre)
            }else if (i==11){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueNovembre)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueNovembre)
            }else if (i==12){
              homme.data() += XYChart.Data[String, Number](i.toString, hommeValueDecembre)
              femme.data() += XYChart.Data[String, Number](i.toString, femmeValueDecembre)
            }

        }
        answer.addAll(homme, femme)
        answer

      }

}