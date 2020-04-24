package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

class BeOmInntektsmeldinger(private val rapidsConnection: RapidsConnection, private val arbeidsgiverProducer: KafkaProducer<String, TrengerInntektsmeldingDTO>) : River.PacketListener{

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "trenger_inntektsmelding") }
            validate { it.requireKey("organisasjonsnummer") }
            validate { it.requireKey("fødselsnummer") }
            validate { it.requireKey("fom") }
            validate { it.requireKey("tom") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("opprettet") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        log.info("Ber om inntektsmelding på vedtaksperiode: {}", packet["vedtaksperiodeId"].asText())

        val payload = TrengerInntektsmeldingDTO(
            organisasjonsnummer = packet["organisasjonsnummer"].asText(),
            fødselsnummer = packet["fødselsnummer"].asText(),
            fom = LocalDate.parse(packet["fom"].asText()),
            tom = LocalDate.parse(packet["tom"].asText()),
            opprettet = LocalDateTime.parse(packet["opprettet"].asText())
        )

        arbeidsgiverProducer.send(ProducerRecord("aapen-helse-spre-arbeidsgiver", payload.fødselsnummer, payload)).get()
        log.info("Publiserte behov for inntektsmelding på vedtak: ${packet["vedtaksperiodeId"].textValue()}")

        rapidsConnection.publish(JsonMessage.newMessage(
            mapOf(
                "@event_name" to "publisert_behov_for_inntektsmelding",
                "@id" to UUID.randomUUID(),
                "vedtaksperiodeId" to packet["vedtaksperiodeId"]
            )
        ).toJson())
    }
}
