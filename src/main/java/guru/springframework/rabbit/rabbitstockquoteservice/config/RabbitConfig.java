package guru.springframework.rabbit.rabbitstockquoteservice.config;


import com.rabbitmq.client.Connection;

import com.rabbitmq.client.Delivery;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PreDestroy;

@Configuration
public class RabbitConfig {
    public static final String QUEUE="quotes";

    @Autowired
    Mono<Connection> connectionMono;
// Connection Mono bean
    @Bean
    Mono<Connection> connectionMono(CachingConnectionFactory factory)
    {
        return Mono.fromCallable(()->
            factory.getRabbitConnectionFactory().newConnection());
    }
// Close connection in Spring lifecycle
    @PreDestroy
    public void close() throws Exception{
        connectionMono.block().close();
    }
//Define sender

    @Bean
    Sender sender(Mono<Connection> mono)
    {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(mono));
    }

    //Defined Receiver
    @Bean
    Receiver receiver(Mono<Connection> mono)
    {

        return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(mono));
    }
//Actual Consumer of message
//    @Bean
//    Flux<Delivery> deliveryFlux(Receiver receiver)
//    {
//        return receiver.consumeAutoAck(QUEUE);
//    }


}
