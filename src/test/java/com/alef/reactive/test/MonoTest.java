package com.alef.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
Reactive Streams
    1- Asynchronous
    2- Non-blocking
    3- Backpressure

    Interfaces:
        * Processor
        * Publisher
        * Subscriber
        * Subscription
 */

@Slf4j
public class MonoTest {

    @Test
    void monoSubscriber() {

        String name = "Alef chaves";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe();

        log.info("_-------------------------");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();

        log.info("Mono {}", mono);
        log.info("Everything working ok!");
    }

    @Test
    void monoSubscribeConsumer() {
        String name = "Alef Chaves";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe(s -> log.info("value: {}", s));
        log.info("-----------------------------------");

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    void monoSubscribeConsumerError() {

        String name = "Alef Chaves";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("Testing mono with happened");
                });

        mono.subscribe(s -> log.info("name {}", s), s -> log.error("Something bas happened"));
        mono.subscribe(s -> log.info("name {}", s), Throwable::printStackTrace);
        log.info("----------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void monoSubscribeConsumerComplete() {
        String name = "Alef Chaves";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("value: {}", s), Throwable::printStackTrace, () -> log.info("Finished"));
        log.info("-----------------------------------");

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoSubscribeConsumerSubscription() {
        String name = "Alef Chaves";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("Finished"),
                Subscription::cancel);

        log.info("-----------------------------------");

//        StepVerifier.create(mono)
//                .expectNext(name.toUpperCase())
//                .verifyComplete();
    }

    @Test
    void monoDoOnMethods() {
        String name = "Alef Chaves";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request"))
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {} ", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {} ", s))
                .doOnSuccess(s -> log.info("doOnSuccess"));

        mono.subscribe(s -> log.info("value: {}", s), Throwable::printStackTrace, () -> log.info("Finished"));
        log.info("-----------------------------------");

//        StepVerifier.create(mono)
//                .expectNext(name.toUpperCase())
//                .verifyComplete();
    }

    @Test
    void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Error error "))
                .doOnError(throwable -> MonoTest.log.error("Error message: {}", throwable.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void monoDoOnErrorResume() {
        String name = "Alef Chaves";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Error error "))
                .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
                .onErrorResume(s -> {
                    log.info("Inside on Error Resume");
                    return Mono.just(name);
                })
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }
}


