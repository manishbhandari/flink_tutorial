/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example

import com.sun.xml.internal.bind.v2.TODO
import models.CabFares
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */


object StreamingJob {
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    // val txt = env.socketTextStream("localhost", 9990)
    // txt.map(s => s).print()

//
//    ///////// READ DATA///////////
//    var txt = env.readTextFile("./src/main/resources/cab-flink_1.txt")
//    var cabfare = env.readTextFile(filePath = "./src/main/resources/yellow_tripdata_2022-03/csv")
//
//
//    //////Cab Ride Proceesing
//    var cabride_collections = txt.map(new MapFunction[String, CabRide] {
//      override def map(row: String): CabRide = CabRide.fromString(row)
//    } )
//
//    var pass_cnt_fun_and_ongoing = cabride_collections.filter(new FilterFunction[CabRide] {
//      override def filter(t: CabRide): Boolean = t.pass_cnt > 0 && t.onGoingTrip == "yes"
//    } )
//
//    var cnt_ids_with_filter = cabride_collections.map(new MapFunction[CabRide, (CabRide,Int)] {
//      override def map(cabride: CabRide): (CabRide,Int) = (cabride,1)
//      })
//    var cnt_ids_with_filter_cnt = cnt_ids_with_filter.keyBy(_._1.id).sum(1)
//
////    Filter on car_type = SUV then count
//    var suv_car_types = cabride_collections.filter(new FilterFunction[CabRide] {
//      override def filter(t: CabRide): Boolean = t.car_type == "SUV"
//    })
//
//    var cnt_suv_types = suv_car_types.map(new MapFunction[CabRide, (CabRide,Int)] {
//      override def map(cabride: CabRide): (CabRide,Int) = (cabride,1)
//    })
//    var total_cnt_suv_type = cnt_suv_types.keyBy(_._1.car_type).sum(position = 1)
//
////    count how many trips destination to secor 31
//    var trips_to_sec_31 = cabride_collections.filter(new FilterFunction[CabRide] {
//      override def filter(t: CabRide): Boolean = t.dropLoc == "Sector 31"
//    })
//    var cnt_trips_to_sec_31 = trips_to_sec_31.map(new MapFunction[CabRide, (CabRide,Int)] {
//      override def map(cabride: CabRide): (CabRide, Int) = (cabride,1)
//    } )
//    var total_trips_to_sec_31 = cnt_suv_types.keyBy(_._1.dropLoc).sum(position = 1)
//
//
//    /////////// Cab Fare Processing//////////
//
//    total_trips_to_sec_31.print("ff")




//    cabride_collections.print("Cab Rides Pipelines")

//    var carobject = Cabride

//    txt.map({(m: String) => (m.split(" ")(0), 1) })
//                .keyBy(0)
//                .sum(1).print()
//    val txt = txt.flatmap().filter().sum()

//    txt.map(new MapFunction[String, Cabride] {
//      override def map(t: String): Cabride = Cabride.fromString(t)
//      var cabObject = txt.map(avroSerializer(string,cabride))
//
//      cabObject.id = 10

//    }).filter(cr => cr.id )





    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
