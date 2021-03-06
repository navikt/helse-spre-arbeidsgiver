package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import java.util.UUID

internal class BeOmInntektsmeldinger(
    private val rapidsConnection: RapidsConnection,
    private val arbeidsgiverProducer: KafkaProducer<String, InntektsmeldingDTO>
) : River.PacketListener{

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "trenger_inntektsmelding")
                it.requireKey("organisasjonsnummer", "fødselsnummer", "vedtaksperiodeId")
                it.require("fom", JsonNode::asLocalDate)
                it.require("tom", JsonNode::asLocalDate)
                it.require("@opprettet", JsonNode::asLocalDateTime)
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        log.info("Ber om inntektsmelding på vedtaksperiode: {}", packet["vedtaksperiodeId"].asText())

        val payload = packet.tilInntektsmeldingDTO()
        arbeidsgiverProducer.send(ProducerRecord(
            "aapen-helse-spre-arbeidsgiver",
            null,
            payload.fødselsnummer,
            payload,
            listOf(RecordHeader("type", payload.meldingstype))
        )).get()
        log.info("Publiserte behov for inntektsmelding på vedtak: ${packet["vedtaksperiodeId"].textValue()}")

        rapidsConnection.publish(JsonMessage.newMessage(
            mapOf(
                "@event_name" to "publisert_behov_for_inntektsmelding",
                "@id" to UUID.randomUUID(),
                "vedtaksperiodeId" to packet["vedtaksperiodeId"]
            )
        ).toJson())
    }

    private fun JsonMessage.tilInntektsmeldingDTO() = InntektsmeldingDTO.trengerInntektsmelding(
        organisasjonsnummer = this["organisasjonsnummer"].asText(),
        fødselsnummer = this["fødselsnummer"].asText(),
        fom = this["fom"].asLocalDate(),
        tom = this["tom"].asLocalDate(),
        opprettet = this["@opprettet"].asLocalDateTime()
    )
}
