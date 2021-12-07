package guru.springframework.rabbit.rabbitstockquoteservice.service;

import com.rabbitmq.client.Delivery;
import guru.springframework.rabbit.rabbitstockquoteservice.config.RabbitConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.Receiver;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class QuoteRunner implements CommandLineRunner {
    private final QuoteGeneratorService service;
    private final QouteMessageSender sender;
    // private final Flux<Delivery> deliveryFlux;
    private final Receiver receiver;

    @Override
    public void run(String... args) throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(50);
        service.fetchQuoteStream(Duration.ofMillis(100l))
                .take(50)
                .log("Get Quotes")
                .flatMap(sender::sendQuoteMessage)
                .subscribe(msg -> {
                    log.debug("Messages sent to queue");
                    countDownLatch.countDown();
                }, throwable -> {
                    log.error("Got Error", throwable);
                }, () -> {
                    log.debug("Done with Streaming");
                });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);

        AtomicInteger receivedCount = new AtomicInteger();
        receiver.consumeAutoAck(RabbitConfig.QUEUE)
                .log("Receiver Messgaes::::")
                .subscribe(msg -> {
                    log.debug("Received Msg #{} - {}", receivedCount.incrementAndGet(), new String(msg.getBody()));

                }, throwable -> {
                    log.error("Get Error on receive msg", throwable);
                }, () -> {
                    log.debug("Complete");
                });
    }
}
