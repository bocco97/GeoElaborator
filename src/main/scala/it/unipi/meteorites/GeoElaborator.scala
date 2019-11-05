package it.unipi.meteorites
import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.security.KeyStore
import java.util.Properties
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManagerFactory}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import sun.net.www.protocol.https.DefaultHostnameVerifier
import scala.annotation.tailrec
import scala.util.Try
import scala.util.parsing.json._

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
      case "United States" => res = "United States of America"
      case "Congo-Brazzaville" => res = "Congo"
      case "DR Congo" => res = "Congo"
      case "Democratic Republic of the Congo" => res = "Congo"
      case "RSA" => res = "South Africa"
      case "" => res = state
      case _ => res = country
    }
    res
  }

  def restCall(lat:String,lon:String,url_0:String,url_1:String, timeout:Int):String={
    val filekeys = this.getClass.getResourceAsStream("/example.jks")
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    val pssw = "changeit".toCharArray
    keyStore.load(filekeys, pssw)
    val tmf = TrustManagerFactory.getInstance("X509")
    tmf.init(keyStore)
    val context = SSLContext.getInstance("TLS")
    context.init(null, tmf.getTrustManagers, new java.security.SecureRandom())
    val sf = context.getSocketFactory
    val url = new URL(url_0 + "lat=" + lat + "&lon=" + lon + url_1)
    HttpsURLConnection.setDefaultHostnameVerifier(new DefaultHostnameVerifier)
    HttpsURLConnection.setDefaultSSLSocketFactory(sf)

    @tailrec
    def readRestResponse(buffer: BufferedReader, finalResult: String, currentLine: Option[String]): String = currentLine match {
      case  None => finalResult
      case Some(line) => readRestResponse(buffer, finalResult + line, Option(buffer.readLine()))
    }

    def performCall(): String = {
      val urlconn = url.openConnection().asInstanceOf[HttpsURLConnection]
      urlconn.setRequestMethod("GET")
      urlconn.setConnectTimeout(timeout)
      urlconn.setReadTimeout(timeout)
      urlconn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11")
      urlconn.setRequestProperty("Accept", "application/json;charset=UTF8")
      urlconn.connect()
      val buffreader = new BufferedReader(new InputStreamReader(urlconn.getInputStream))
      readRestResponse(buffreader, "", Option(buffreader.readLine()))
    }

    val jsonResponse = Try(performCall()).getOrElse(AdditionalInfo.empty)
    jsonResponse
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
      val state = info.getOrElse("state", "")
      val country = normalize(info.getOrElse("country",""),state)
      val name = response.getOrElse("display_name","").replace(',','-')
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

  def write2Hbase(table_name:String, col_fam:String, meteorite:Meteorite,zq:String,port:String):Unit={
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum",zq)
    config.set("hbase.zookeeper.property.clientPort",port)
    val conn = ConnectionFactory.createConnection(config)
    val table = conn.getTable(TableName.valueOf(table_name))
    var put = new Put(Bytes.toBytes(meteorite.year+"_"+scala.util.Random.nextInt(scala.Int.MaxValue)))
    val list = Meteorite.getFields()
    list.foreach(f=>
      {
        val col_bytes = Bytes.toBytes(col_fam)
        val f_bytes = Bytes.toBytes(f)
        val met_bytes = Bytes.toBytes(Meteorite.get(meteorite,f))
        put = put.addColumn(col_bytes,f_bytes,met_bytes)
      }
    )
    table.put(put)
    table.close()
    conn.close()
  }

  def main(args: Array[String]): Unit = {
    /*************************************** Parametri di configurazione ***********************************/
    val url_0 = "https://nominatim.openstreetmap.org/reverse?format=json&"
    val url_1 = "&zoom=5&addressdetails=1&accept-language=<en>"
    val prop = new Properties()
    prop.load(this.getClass().getResourceAsStream("/config.properties"))
    val LOCAL_RUN = prop.getProperty("LOCAL_RUN")
    val BOOTSTRAP_SERVER = prop.getProperty("BOOTSTRAP_SERVER")
    val GROUP_ID = prop.getProperty("GROUP_ID")
    val AUTO_OFFSET_RESET = prop.getProperty("AUTO_OFFSET_RESET")
    val TOPIC = prop.getProperty("READING_TOPIC")
    val WRITING_TOPIC = prop.getProperty("WRITING_TOPIC")
    val REQ_TIMEOUT =  prop.getProperty("REST_REQ_TIMEOUT").toInt*1000
    val HBASE_TABLE = prop.getProperty("HBASE_TABLE")
    val COL_FAM = prop.getProperty("COLUMN_FAMILY")
    val ZOOKEEPER_QUORUM = prop.getProperty("ZOOKEEPER_QUORUM")
    val ZK_CLIENT_PORT = prop.getProperty("ZOOKEEPER_CLIENT_PORT")
    /******************************************************************************************************/
    val conf = if (LOCAL_RUN=="true") new SparkConf().setMaster("local[*]") else new SparkConf()
    val sc = new SparkContext(conf.setAppName("GeoElaborator"))
    val ssc = new StreamingContext(sc, Seconds(1))
    val kafkaParams =Map[String,String](        "bootstrap.servers" -> BOOTSTRAP_SERVER,
                                      "group.id" -> GROUP_ID,
                                      "auto.offset.reset" -> AUTO_OFFSET_RESET
                                      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = List(TOPIC).toSet
    val record = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    val prod = createKafkaProducer(BOOTSTRAP_SERVER)
    val record1 = record.transform(rdd => rdd).map(_._2)
    record1.foreachRDD(x => {
        try {
          val evt = x.first()
          val map = string2map(evt)
          val lat = map("reclat")
          val lon = map("reclong")
          val result = restCall(lat, lon, url_0, url_1, REQ_TIMEOUT)
          val meteorite = elaborate(result, evt)
          val met2js = Meteorite.toJson(meteorite)
          val record = new ProducerRecord[String, String](WRITING_TOPIC,met2js)
          write2Hbase(HBASE_TABLE, COL_FAM, meteorite,ZOOKEEPER_QUORUM,ZK_CLIENT_PORT)
          prod.send(record, new Callback() {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              e match {
                case null => {
                  log.info("\n\n**sent: " + met2js + "**\n")
                }
                case _ => {
                  log.warn("Error sending meteorite data to Kafka")
                }
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