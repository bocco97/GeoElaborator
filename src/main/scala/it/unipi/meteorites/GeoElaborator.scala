package it.unipi.meteorites
import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.security.KeyStore
import java.util.Properties
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManagerFactory}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import sun.net.www.protocol.https.DefaultHostnameVerifier
import scala.util.parsing.json._

//spark-submit --master yarn --deploy-mode client --class "GeoElaborator" GeoElaborator-1.0-SNAPSHOT.jar
//spark-submit --master yarn --deploy-mode cluster --class it.unipi.meteorites.GeoElaborator GeoElaborator-1.0-SNAPSHOT.jar
object GeoElaborator {

  val log = LoggerFactory.getLogger(GeoElaborator.getClass.getName)

  def string2map(s:String):Map[String,String]={
    val obj = JSON.parseFull(s)
    val map = obj.get.asInstanceOf[Map[String, String]]
    map
  }

  def normalize(country: String, state: String):String={
    var res = country
    country match{
      case "PRC"=> res = "China"
      case "USA" => res = "United States of America"
      case "Congo-Brazzaville" => res = "Congo"
      case "DR Congo" => res = "Congo"
      case "" => res = state
      case _ => res = country
    }
    res
  }

  def restCall(lat:String,lon:String,url_0:String,url_1:String):String={
    val filekeys = this.getClass.getResourceAsStream("/example.jks")
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    val pssw = Array('c', 'h', 'a', 'n', 'g', 'e', 'i', 't')
    keyStore.load(filekeys, pssw)
    val tmf = TrustManagerFactory.getInstance("X509")
    tmf.init(keyStore)
    val context = SSLContext.getInstance("TLS")
    context.init(null, tmf.getTrustManagers, new java.security.SecureRandom())
    val sf = context.getSocketFactory
    val url = new URL(url_0 + "lat=" + lat + "&lon=" + lon + url_1)
    HttpsURLConnection.setDefaultHostnameVerifier(new DefaultHostnameVerifier)
    HttpsURLConnection.setDefaultSSLSocketFactory(sf)
    val urlconn = url.openConnection().asInstanceOf[HttpsURLConnection]
    urlconn.setRequestMethod("GET")
    urlconn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11")
    urlconn.setRequestProperty("Accept", "application/json;charset=UTF8")
    urlconn.connect()
    var res = ""
    try {
      val buffreader = new BufferedReader(new InputStreamReader(urlconn.getInputStream))
      var line = buffreader.readLine()
      while (line != null) {
        res = res + line
        line = buffreader.readLine()
      }
      buffreader.close()
    }
    catch{
      case x :java.io.IOException => log.error("error in response",x)
    }
    res
  }

  def elaborate(result:String, event:String):Meteorite={
    val response = string2map(result)
    val evt = string2map(event)
    if(response.get("error").isDefined){
      val year = evt.get("year").get
      val mass = String.valueOf(evt.get("mass").get)
      val reclat = evt.get("reclat").get
      val reclon = evt.get("reclong").get
      val reccl = evt.get("recclass").get
      val m = Meteorite(year,mass,reclat,reclon,reccl,false,null)
      m
    }
    else{
      val info = response.get("address").get.asInstanceOf[Map[String,String]]
      var state = ""
      var country = ""
      try {
        state = info.get("state").get
      }
      catch{
        case _ : Throwable => ()
      }
      try {
       country = info.get("country").get
      }
      catch{
        case _ : Throwable => ()
      }
      country= normalize(country,state)
      val name = response.get("display_name").get.replace(',','-')
      val additionalInfo = AdditionalInfo(name,state,country)
      val year = evt.get("year").get
      val mass = String.valueOf(evt.get("mass").get)
      val reclat = evt.get("reclat").get
      val reclon = evt.get("reclong").get
      val reccl = evt.get("recclass").get
      val m = Meteorite(year,mass,reclat,reclon,reccl,true,additionalInfo)
      m
    }
  }

  def createKafkaProducer(server:String):KafkaProducer[String, String]={
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String,String](properties)
  }

  def main(args: Array[String]): Unit = {
    /*************************************** Parametri di configurazione ***********************************/
    var localRun = ""
    var bootstrap_server = ""
    var group_id = ""
    var auto_offset_reset = ""
    var topic = ""
    var writing_topic = ""
    /******************************************************************************************************/
    val url_0 = "https://nominatim.openstreetmap.org/reverse?format=json&"
    val url_1 = "&zoom=5&addressdetails=1&accept-language=<en>"
    val prop = new Properties()
    prop.load(this.getClass().getResourceAsStream("/config.properties"))
    localRun = prop.getProperty("LOCAL_RUN")
    bootstrap_server = prop.getProperty("BOOTSTRAP_SERVER")
    group_id = prop.getProperty("GROUP_ID")
    auto_offset_reset = prop.getProperty("AUTO_OFFSET_RESET")
    topic = prop.getProperty("READING_TOPIC")
    writing_topic = prop.getProperty("WRITING_TOPIC")
    val conf = if (localRun=="true") new SparkConf().setMaster("local[*]") else new SparkConf()
    val sc = new SparkContext(conf.setAppName("GeoElaborator"))
    val ssc = new StreamingContext(sc, Seconds(1))
    val kafkaParams =Map[String,String](        "bootstrap.servers" -> bootstrap_server,
                                      "group.id" -> group_id,
                                      "auto.offset.reset" -> auto_offset_reset
                                      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = List(topic).toSet
    val record = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    val prod = createKafkaProducer(bootstrap_server)
    val record1 = record.transform(rdd => rdd).map(_._2)
    record1.foreachRDD(x => {
        try {
          val evt = x.first()
          val map = string2map(evt)
          val lat = map.get("reclat").get
          val lon = map.get("reclong").get
          val result = restCall(lat, lon, url_0, url_1)
          val meteorite = elaborate(result,evt)
          val met2js = Meteorite.toJson(meteorite)
          val record = new ProducerRecord[String,String](writing_topic,met2js)
          prod.send(record, new Callback(){
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              if(e==null){
                log.info("\n\n**sent: "+met2js+"**\n")
              }
              else{
                log.warn("Error sending meteorite data to Kafka")
              }
            }
          })
        }
        catch{
          case _:java.lang.UnsupportedOperationException =>{
            log.info("Nothing to elaborate. Waiting . . .")
          }
        }
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
