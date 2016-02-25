package main.scala

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.io.Source
import akka.util.ByteStringBuilder
import jssc._
import jssc.SerialPortException

object KafkaProduceTest {

  val brokerlist = "localhost:9092" //change to your remote cluster IP
  val topic = "connect-test"     //Set your topic
  val producer = getNewProducer(brokerlist) 

  def main(args: Array[String]) {
    val serialPort = new SerialPort("COM1");
    System.out.println("Port opened: " + serialPort.openPort());
    System.out.println("Params setted: " + serialPort.setParams(9600, 8, 1, 0, false, false));

    println("kafkaStarting")
    run(serialPort)
  }

  def getNewProducer(brokerList: String): KafkaProducer[String, String] = {
    val kafkaProps = new Properties
    kafkaProps.put("bootstrap.servers", brokerList)
    // kafkaProps.put("metadata.broker.list", brokerList)

    // This is mandatory, even though we don't send keys
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", "0")

    // how many times to retry when produce request fails?
    kafkaProps.put("retries", "3")
    kafkaProps.put("linger.ms", "2")
    kafkaProps.put("batch.size", "1000")
    //kafkaProps.put("queue.time", "2")

    return new KafkaProducer[String, String](kafkaProps)
  }

  def run(serialChannel: SerialPort) {

    while (true) {
      val data = serialChannel.readString()
      if (data != null) {
        println(data)
        val message = new ProducerRecord[String, String](topic, data)
        producer.send(message)
        producer.close()
      }
    }
  }
}