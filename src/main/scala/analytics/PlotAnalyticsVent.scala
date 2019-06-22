package analytics

import fr.addinn.analytics.PlotAnalyticsAccidents.stage
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



object PlotAnalyticsVent extends JFXApp {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder.master("local[*]").appName("transport").getOrCreate()

  val Meteotransport = spark.read.option("header", true).option("inferSchema", true).option("timestampFormat", "dd/MM/yyyy").format("csv").load("meteotran.csv")
  import spark.implicits._

  var vitesseValueVentInf9Janvier = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 1)).count()
  var vitesseValueVentInf20Janvier = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 1)).count()
  var vitesseValueVentInf37Janvier = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 1)).count()


  var vitesseValueVentInf9Fevrier = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 2)).count()
  var vitesseValueVentInf20Fevrier = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 2)).count()
  var vitesseValueVentInf37Fevrier = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 2)).count()


  var vitesseValueVentInf9Mars = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 3)).count()
  var vitesseValueVentInf20Mars = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 3)).count()
  var vitesseValueVentInf37Mars = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 3)).count()


  var vitesseValueVentInf9Avril = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 4)).count()
  var vitesseValueVentInf20Avril = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 4)).count()
  var vitesseValueVentInf37Avril = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 4)).count()


  var vitesseValueVentInf9Mai = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 5)).count()
  var vitesseValueVentInf20Mai = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 5)).count()
  var vitesseValueVentInf37Mai = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 5)).count()


  var vitesseValueVentInf9Juin = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 6)).count()
  var vitesseValueVentInf20Juin = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 6)).count()
  var vitesseValueVentInf37Juin = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 6)).count()


  var vitesseValueVentInf9Juillet = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 7)).count()
  var vitesseValueVentInf20Juillet = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 7)).count()
  var vitesseValueVentInf37Juillet = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 7)).count()


  var vitesseValueVentInf9Aout = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 8)).count()
  var vitesseValueVentInf20Aout = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 8)).count()
  var vitesseValueVentInf37Aout = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 8)).count()


  var vitesseValueVentInf9Septembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 9)).count()
  var vitesseValueVentInf20Septembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 9)).count()
  var vitesseValueVentInf37Septembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 9)).count()


  var vitesseValueVentInf9Octobre = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 10)).count()
  var vitesseValueVentInf20Octobre = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 10)).count()
  var vitesseValueVentInf37Octobre = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 10)).count()


  var vitesseValueVentInf9Novembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 11)).count()
  var vitesseValueVentInf20Novembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 11)).count()
  var vitesseValueVentInf37Novembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 11)).count()


  var vitesseValueVentInf9Decembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 0 and ($"Vitessevent10mn" < 5) and ($"mois" === 12)).count()
  var vitesseValueVentInf20Decembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 5.0 and ($"Vitessevent10mn" < 9.9) and ($"mois" === 12)).count()
  var vitesseValueVentInf37Decembre = Meteotransport.select("*").filter($"Vitessevent10mn" > 10.0 and ($"Vitessevent10mn" < 14.9) and ($"mois" === 12)).count()

  val xAxis = new CategoryAxis()
  val yAxis = new NumberAxis()
  val barChart = BarChart(xAxis, yAxis)
  barChart.title = "Nombre d'accident 2017 par rapport au vitesse du vent"
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
    val zeroneuf = new XYChart.Series[String, Number] {
      name = "< 9 km/h"
    }
    val neufvingt = new XYChart.Series[String, Number] {
      name = "9-19 km/h"
    }
    val vingttrentesept = new XYChart.Series[String, Number] {
      name = "> 19 km/h"
    }

    for (i <- 1 to 12 ) {

      if ( i ==1){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Janvier)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Janvier)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Janvier)

      }else if (i==2){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Fevrier)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Fevrier)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Fevrier)

      }else if (i==3){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Mars)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Mars)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Mars)

      }else if (i==4){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Avril)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Avril)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Avril)

      }else if (i==5){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Mai)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Mai)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Mai)
      }else if (i==6){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Juin)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Juin)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Juin)

      }
      else if (i==7){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Juillet)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Juillet)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Juillet)

      }else if (i==8){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Aout)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Aout)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Aout)

      }else if (i==9){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Septembre)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Septembre)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Septembre)

      }else if (i==10){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Octobre)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Octobre)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Octobre)

      }else if (i==11){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Novembre)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Novembre)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Novembre)

      }else if (i==12){
        zeroneuf.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf9Decembre)
        neufvingt.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf20Decembre)
        vingttrentesept.data() += XYChart.Data[String, Number](i.toString, vitesseValueVentInf37Decembre)

      }

    }
   answer.addAll(zeroneuf,neufvingt,vingttrentesept)
    answer

  }

}
