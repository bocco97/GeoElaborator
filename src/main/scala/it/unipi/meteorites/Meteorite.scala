package it.unipi.meteorites
import org.json4s._
import org.json4s.native.Serialization.{read, write}


case class Meteorite(year:String, mass: String, lat:String, lon:String, recclass:String, resolved:Boolean, info:AdditionalInfo)



object Meteorite{
  implicit val formats = DefaultFormats
  def toJson(m:Meteorite): String ={
    val str = write(m)
    str
  }
  def getFields(): List[String]={
    List[String]("mass","lat","lon","recclass","resolved","info")
  }
  def get(m:Meteorite,f:String): String ={
    f match {
      case "mass" => m.mass
      case "lat" => m.lat
      case "lon" => m.lon
      case "recclass" => m.recclass
      case "resolved" => m.resolved.toString
      case "info" => {
        if (m.resolved) m.info.toString
        else "null"
      }
      case _ => ""
    }
  }
}
