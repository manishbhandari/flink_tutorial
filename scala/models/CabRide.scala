package models

//import java.util.Date
import models.GeoUtils

class CabRide {
  var medallion : String      = _
  var hack_license: String      = _
  var vendor_id: String      = _
  var rate_code: String      = _
  var store_and_fwd_flag: String      = _
  var pickup_datetime: String      = _
  var dropoff_datetime: String      = _
  var passenger_count: String      = _
  var trip_time_in_secs: String      = _
  var trip_distance: String      = _
  var pickup_longitude: Double      = _
  var pickup_latitude: Double      = _
  var dropoff_longitude: Double      = _
  var dropoff_latitude : Double      = _
  var isNYC_pickup: Int = 0
  var isNYC_drop: Int = 0

  override def toString: String = (medallion + " " + hack_license + " " + vendor_id + " "
    + rate_code + " "
    + store_and_fwd_flag + " "
    + pickup_datetime + " "
    + dropoff_datetime + " "
    + passenger_count + " "
    + trip_time_in_secs + " "
    + trip_distance + " "
    + pickup_longitude + " "
    + pickup_latitude + " "
    + dropoff_longitude + " "
    + dropoff_latitude + " "
//    + isNYC_pickup + " "
//    + isNYC_drop
    )

}



object CabRide {
//  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => 0.0 }


  def fromString(s: String) : CabRide = {
    val ride= new CabRide
    val tokens = s.split(",")
    ride.medallion = tokens(0)
    ride.hack_license = tokens(1)
    ride.vendor_id = tokens(2)
    ride.rate_code= tokens(3)
    ride.store_and_fwd_flag= tokens(4)
    ride.pickup_datetime= tokens(5)
    ride.dropoff_datetime= tokens(6)
    ride.passenger_count= tokens(7)
    ride.trip_time_in_secs= tokens(8)
    ride.trip_distance= tokens(9)

    ride.pickup_longitude=  try{ tokens(10).toDouble} catch  {case _: NumberFormatException => 0.0}
//      parseDouble(tokens(10))
//      if (tokens(10).isEmpty) {0} else tokens(10).toDouble
    ride.pickup_latitude= try{ tokens(11).toDouble} catch  {case _: NumberFormatException => 0.0}
    ride.dropoff_longitude= try{ tokens(12).toDouble} catch  {case _: NumberFormatException => 0.0}
    ride.dropoff_latitude= try{ tokens(13).toDouble} catch  {case _: NumberFormatException => 0.0}
//    ride.isNYC_pickup = if (GeoUtils.isNYC(ride.pickup_latitude.toDouble,ride.pickup_longitude.toDouble)) {1 } else 0
//    ride.isNYC_drop = if (GeoUtils.isNYC(ride.dropoff_latitude.toDouble,ride.dropoff_longitude.toDouble)) {1 } else 0
    return ride

  }


}