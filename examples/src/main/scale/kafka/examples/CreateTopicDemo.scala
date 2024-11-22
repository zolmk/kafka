package kafka

import org.apache.kafka.clients.admin.AdminClient

import java.util
import java.util.Collections

object CreateTopicDemo {
  def main(args: Array[String]): Unit = {
    val props = new util.HashMap[String, Object]()
    props.put("bootstrap.servers", "127.0.0.1:9093")
    val adminClient = {
      AdminClient.create(props)
    }
    adminClient.createTopics(Collections.singleton("test-topic")).all().get()
  }
}