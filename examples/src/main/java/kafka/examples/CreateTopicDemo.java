package kafka.examples;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
public class CreateTopicDemo {
		public static void main(String[] args) throws ExecutionException, InterruptedException {
				Map<String, Object> props = new HashMap<>();
				props.put("bootstrap.servers", "127.0.0.1:9093");
				AdminClient adminClient = AdminClient.create(props);
				adminClient.createTopics(Collections.singletonList(new NewTopic("test-topic", 2,
					(short) -1))).all().get();
		}
}
