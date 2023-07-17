package org.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import models.{CabRide, GeoUtils}
import org.apache.flink.api.common.functions.FilterFunction
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//import models.GeoUtils
import org.apache.flink.api.common.functions.MapFunction

//import com.sun.xml.internal.bind.v2.TODO
//import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._
//import

object CabRideStreamingJob {

  class MyProcessWindowFunction extends ProcessWindowFunction[CabRide, String, String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[CabRide], out: Collector[String]) = {
      var count = 0L
      for (in <- input) {
        count = count + 1
      }
      out.collect(s"Window ${context.window} count: $count")
    }
  }


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(10000)
    env.setParallelism(1)
    var txt = env.readTextFile("./src/main/resources/trip_data_1k.csv")


    var cabRides_collections = txt.map(new MapFunction[String, CabRide] {
      override def map(row: String): CabRide = CabRide.fromString(row)
    })


    var cabRides_inNYC = cabRides_collections.filter(new FilterFunction[CabRide] {
      override def filter(t: CabRide): Boolean = (GeoUtils.isNYC(t.pickup_latitude.toDouble, t.pickup_longitude.toDouble) & GeoUtils.isNYC(t.dropoff_latitude.toDouble, t.dropoff_longitude.toDouble))
    }).map(new MapFunction[CabRide,(String,Int)] {
      override def map(t: CabRide): (String, Int) =  (t.hack_license, t.passenger_count.toInt)
    }).filter(new FilterFunction[(String, Int)] {
      override def filter(t: (String, Int)): Boolean = (t._2.toInt > 1)
    }).keyBy(0).sum(1)

//    Filter all streams where the ride is ongoing right now
    var cabRides_ongoing = cabRides_collections.filter(new FilterFunction[CabRide] {
      override def filter(t: CabRide): Boolean = t.dropoff_datetime.isEmpty()
    })

//    count of rides for each driver
    var cabRides_totalridesperdriver = cabRides_collections.map(new MapFunction[CabRide, (String,Int, Int)] {
      override def map(t: CabRide): (String,Int, Int) = try{(t.hack_license,t.hack_license.toInt / 10,1)} catch {case _: NumberFormatException => (t.hack_license,0,1)}
    }).keyBy(1).sum(2).print()






//    cabRides_totalridesperdriver.print()


    env.execute()

  }
}
