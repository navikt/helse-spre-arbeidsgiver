package no.nav.helse

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.Application
import io.ktor.application.install
import io.ktor.features.ContentNegotiation
import io.ktor.jackson.jackson
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("sprearbeidsgiver")

@FlowPreview
fun main() = runBlocking(Executors.newFixedThreadPool(4).asCoroutineDispatcher()) {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    val server = embeddedServer(Netty, 8080) {
        installJacksonFeature()
        install(MicrometerMetrics) {
            registry = meterRegistry
        }

        routing {
            registerHealthApi({ true }, { true }, meterRegistry)
        }
    }.start(wait = false)

    val rapidConsumer =
        KafkaConsumer<ByteArray, JsonNode?>(loadBaseConfig(environment, serviceUser).toConsumerConfig())
    val arbeidsgiverProducer =
        KafkaProducer<String, TrengerInntektsmeldingDTO>(loadBaseConfig(environment, serviceUser).toProducerConfig())

    rapidConsumer
        .subscribe(listOf(environment.rapidTopic))

    rapidConsumer.asFlow()
        .inntektsmeldingFlow()
        .collect { value ->
            arbeidsgiverProducer.send(ProducerRecord(environment.sprearbeidsgivertopic, value.fnr, value)).get()
                .also { log.info("Publiserer behov for inntektsmelding") }
        }


    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(10, 10, TimeUnit.SECONDS)
    })
}

fun Flow<Pair<ByteArray, JsonNode?>>.inntektsmeldingFlow() = this
    .map { it.second }
    .filterNotNull()
    .filter { value ->
        value["@event_name"].asText() == "trenger_inntektsmelding"
    }
    .onEach { value -> log.info("Ber om inntektsmelding på vedtaksperiode: {}", value["vedtaksperiodeId"].asText())}
    .map { value ->
        TrengerInntektsmeldingDTO(
            orgnummer = value["organisasjonsnummer"].asText(),
            fnr = value["fødselsnummer"].asText(),
            fom = LocalDate.parse(value["fom"].asText()),
            tom = LocalDate.parse(value["tom"].asText()),
            opprettet = LocalDateTime.parse(value["opprettet"].asText())
        )
    }

internal fun Application.installJacksonFeature() {
    install(ContentNegotiation) {
        jackson {
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            registerModule(JavaTimeModule())
        }
    }
}

data class TrengerInntektsmeldingDTO(
    val orgnummer: String,
    val fnr: String,
    val fom: LocalDate,
    val tom: LocalDate,
    val opprettet: LocalDateTime
)
