package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.convertValue
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.time.LocalDate
import java.util.*
import kotlin.test.assertEquals

class AppTest {

    @Test
    fun test() = runBlocking {
        val fnr = "12345678910"
        val orgnr = "987654321"
        val json = asJsonNode(
            mapOf(
                "@event_name" to "trenger_inntektsmelding",
                "vedtaksperiodeId" to UUID.randomUUID(),
                "fødselsnummer" to fnr,
                "organisasjonsnummer" to orgnr,
                "opprettet" to LocalDate.now(),
                "fom" to LocalDate.now(),
                "tom" to LocalDate.now()
            )
        )
        val expected = listOf(
            TrengerInntektsmeldingDTO(orgnummer = orgnr, fnr = fnr),
            TrengerInntektsmeldingDTO(orgnummer = orgnr, fnr = fnr)
        )
        val actual = listOf(
            ("123".toByteArray() to json),
            ("234".toByteArray() to null),
            ("345".toByteArray() to json)
        ).asFlow().inntektsmeldingFlow().toList()
        assertEquals(expected, actual)
    }

    private fun asJsonNode(values: Map<String, Any>) = objectMapper.convertValue<JsonNode>(values)

}
