package it.unipi.meteorites

case class AdditionalInfo(Name: String, State:String, Country:String)

object AdditionalInfo {
  def empty = "{\"address\":{\"display_name\":\"Unknown\",\"state\":\"Unknown\",\"country\":\"Unknown\"}}"
}