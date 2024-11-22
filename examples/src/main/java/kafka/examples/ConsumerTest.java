package kafka.examples;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
public class ConsumerTest {
		public static void main(String[] args) throws InterruptedException {
				CountDownLatch latch = new CountDownLatch(3);
				Consumer consumer = new Consumer("consumer", "127.0.0.1:9093", "test", "test-group",
					Optional.empty(), true, 1, latch);
				consumer.start();
				latch.await();
		}
}
