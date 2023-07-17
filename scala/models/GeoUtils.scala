package models

class GeoUtils
{
  val LON_EAST: Double = -73.7
  val LON_WEST: Double = -74.05
  val LAT_NORTH: Double  = 41.0
  val LAT_SOUTH: Double  = 40.5


}

object GeoUtils {

  def isNYC(lat:Double , lon:Double ): Boolean ={
    val geout = new GeoUtils
    return !(lon > geout.LON_EAST || lon < geout.LON_WEST) && !(lat > geout.LAT_NORTH || lat < geout.LAT_SOUTH)

  }


}