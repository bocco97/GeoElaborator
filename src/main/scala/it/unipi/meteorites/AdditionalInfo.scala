package it.unipi.meteorites
import org.json4s._
import org.json4s.jackson.Serialization.{read, write}

case class AdditionalInfo(Name: String, State:String, Country:String)

object AdditionalInfo{
  implicit val formats = DefaultFormats
  def toJson(a:AdditionalInfo):String={
    val str = write(a)
    str
  }
}