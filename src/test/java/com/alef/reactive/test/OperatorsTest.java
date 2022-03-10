package com.alef.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

@Slf4j
public class OperatorsTest {

    /*
        Quando não dizemos para o reactor  usar concorrencia,
         ele vai executar na main thread
     */




    /*
    o subscribeOn afeta todo o fluxo de eventos,
    independente de onde ele está inserido!

     */
    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1,5)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i , Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Tgread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    /*
        PARA EXEMPLO ABAIXO
            -> O subscribeOn vai ser aplicado no processo de subscricao
            ele afeta todo o chain de eventos, não importa onde esteja

            -> O publishOn só vai afetar o que está abaixo de onde ele esta

     */

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1,5)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i , Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Tgread {}", i, Thread.currentThread().getName());
                    return i;
                });
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1,5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i , Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Tgread {}", i, Thread.currentThread().getName());
                    return i;
                });
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1,5)
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i , Thread.currentThread().getName());
                    return i;
                })
                . publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 2 - Number {} on Tgread {}", i, Thread.currentThread().getName());
                    return i;
                });
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1,5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i , Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Tgread {}", i, Thread.currentThread().getName());
                    return i;
                });
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void subscribeOnIO() {
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(s -> log.info("{}", s));

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    log.info("Size {}", l.size());
                    return true;
                })
                .verifyComplete();
    }

    //---------------------------------------------------------------------//

    /*
            SwitchIfEmptyOperator ()
            --> É um operador que podemos utilizar quando
            estamos dando um subscribe em um Flux, e aquele Flux não tem nada para retornar
            --> funciona como um if else, ou seja, if Flux.isEmpty(), então podemos usar um
            outro valor que definimos dentro do swithIfEmpty

     */


    @Test
    public void switIfEmptyOperator() {
        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("Not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("Not empty anymore")
                .expectComplete()
                .verify();
    }

    //----------------------------------------------------------//

    /*[
    com palavras simples se você vê na primeira visualização é como Mono.just() mas não é. quando você executa Mono.just(),
    ele cria imediatamente um Observable(Mono) e o reutiliza,
     mas quando você usa defer, ele não o cria imediatamente, ele cria um novo Observable em cada subscription.
     */

    @Test
    public void deferOperator() throws InterruptedException {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);

    }

    //------------------------------------------------//

    @Test
    public void concatOperator() {
        Flux<String> flux1 = Flux.just("A", "B");
        Flux<String> flux2 = Flux.just("C", "D");

        Flux<String> fluxConcat = Flux.concat(flux1, flux2).log();

        StepVerifier.create(fluxConcat)
                .expectSubscription()
                .expectNext("A", "B", "C", "D")
                .expectComplete()
                .verify();

    }

    @Test
    public void concatOperatorError() {
        Flux<String> flux1 = Flux.just("A", "B")
                .map(s -> {
                    if (s.equals("B")){
                        throw new IllegalArgumentException();
                    }
                    return s;
                });

        Flux<String> flux2 = Flux.just("C", "D");

        Flux<String> fluxConcat = Flux.concat(flux1, flux2).log();

        StepVerifier.create(fluxConcat)
                .expectSubscription()
                .expectNext("A")
                .expectError()
                .verify();

    }

    @Test
    public void concatWithOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("A", "B");
        Flux<String> flux2 = Flux.just("C", "D");

        Flux<String> fluxConcatWith = flux1.concatWith(flux2).log();


        StepVerifier.create(fluxConcatWith)
                .expectSubscription()
                .expectNext("A", "B", "C", "D")
                .expectComplete()
                .verify();

    }


    @Test
    void combineLatestOperator() {
        Flux<String>  flux1 = Flux.just("A", "B");
        Flux<String>  flux2 = Flux.just("C", "D");

        Flux<String> combineLatest = Flux.combineLatest(flux1, flux2,
                (s1, s2) -> s1.toLowerCase() + s2.toLowerCase()).log();

        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("bc", "bd")
                .expectComplete()
                .verify();
    }
    
    
    //-------------------------------------------------//


    @Test
    void mergeOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("C", "D");

        //esse aceita diversos flux
        Flux<String> mergeFlux = Flux.merge(flux1, flux2).map(i -> i.toLowerCase()).log();

        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a","b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeWithOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("C", "D");

        //esse aceita que seja feito merge em mais um flux
        Flux<String> mergeFlux = flux1.mergeWith(flux2).log();

        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a","b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeSequentialOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeSequential(flux1, flux2, flux1)
                .delayElements(Duration.ofMillis(200)).log();
        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a","b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }
}
