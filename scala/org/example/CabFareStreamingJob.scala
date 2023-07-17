package org.example
import com.sun.xml.internal.bind.v2.TODO
import models.{CabFares, Case_CabFares, GeoUtils}
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._

import java.lang
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object CabFareStreamingJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    var txt = env.readTextFile("./src/main/resources/trip_fare_1k.csv")

    var cabFares_collections = txt.map(new MapFunction[String, CabFares] {
      override def map(row: String): CabFares = CabFares.fromString(row)

    })

    //    Find the total tips for a driver
//    var tot_tips = cabFares_collections.map(new MapFunction[CabFares, (Int,Double)] {
//      override def map(t: CabFares): (Int, Double) = (t.hack_license.toInt, t.tip_amount.toDouble)
//    }).keyBy(0).sum(1).print()



    //    Find the total tips for a driver over last 1 day - keyby, window, apply aggregation

    var case_CabFares_Collections = txt.map(new MapFunction[String, Case_CabFares]
    {
      override def map(row: String): Case_CabFares = {
        val token = row.split(",")
        return Case_CabFares(token(0), token(1),token(2),token(3),token(4),token(5),token(6),token(7),token(8).toDouble,token(9),token(10))

      }
    }
    )


    var driver_hr = case_CabFares_Collections
      .keyBy("hack_license")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//      .process(new ProcessWindowFunction[Case_CabFares,String,S] {})
      .process(new ProcessWindowFunction[Case_CabFares,String,String,TimeWindow]
      {
//        override def process(key: String, context: Context, elements: Iterable[Case_CabFares], out: Collector[String]): Unit = {
//        for ( x <- elements )
//        {
//          print(x)
//
//        }
//
//      }

        override def process(key: String, context: ProcessWindowFunction[Case_CabFares, String, String, TimeWindow]#Context, iterable: lang.Iterable[Case_CabFares], collector: Collector[String]): Unit = ???
      })


//      .sum("tip_amount")
    driver_hr.print()

//      .window(TumblingEventTimeWindows.of(Time.seconds(10)))





    //    cabFares_collections.print()
    env.execute()
  }
}





