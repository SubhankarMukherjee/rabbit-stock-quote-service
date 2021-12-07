package guru.springframework.rabbit.rabbitstockquoteservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import guru.springframework.rabbit.rabbitstockquoteservice.config.RabbitConfig;
import guru.springframework.rabbit.rabbitstockquoteservice.model.Quote;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Sender;
@Slf4j
@Component
@RequiredArgsConstructor
public class QouteMessageSender {

    private final ObjectMapper objectMapper;
    private final Sender sender;

    @SneakyThrows
    public Mono<Void> sendQuoteMessage(Quote quote)
    {
        byte[] jsonBytes= objectMapper.writeValueAsBytes(quote);
        //return sender.send(Mono.just(new OutboundMessage("", RabbitConfig.QUEUE,jsonBytes)));


        // More Sophisticate way to send messages

        Flux<OutboundMessageResult> confirms = sender.sendWithPublishConfirms(Flux.just(new OutboundMessage("", RabbitConfig.QUEUE, jsonBytes)));
            sender.declareQueue(QueueSpecification.queue(RabbitConfig.QUEUE))
                    .thenMany(confirms)
                    .doOnError(r->log.error("send failed", r))
                    .subscribe(r->{
                        if(r.isAck())
                        {
                            log.debug("Message sent successfully {}",new String(r.getOutboundMessage().getBody()));
                        }
                    });
            return Mono.empty();



    }

}
