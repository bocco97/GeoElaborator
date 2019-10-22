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
}
