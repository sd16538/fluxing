package personal.sdm.fluxing

import com.tyro.oss.randomdata.RandomBoolean.randomBoolean
import com.tyro.oss.randomdata.RandomString.randomAlphabeticString
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@SpringBootApplication
class FluxingApplication

fun main(args: Array<String>) {
    runApplication<FluxingApplication>(*args)
}

@RestController
class FluxController {

    @GetMapping("/magic0")
    @ResponseStatus(HttpStatus.OK)
    fun `get all results combined`(): CombinedResult = MonoService().gimmeMono().block()!!


    @GetMapping("/magic")
    @ResponseStatus(HttpStatus.OK)
    fun `get all results combined as mono`(): Mono<CombinedResult> = MonoService().gimmeMono()

    @GetMapping("/magic2")
    @ResponseStatus(HttpStatus.OK)
    fun `return combined as mono`(): Unit {
        val gimmeMono = MonoService().gimmeMono2()
        println("got mono")

        gimmeMono.subscribe { thing -> println("Here is mono [${thing}]") }
    }

    @GetMapping("/magic3")
    @ResponseStatus(HttpStatus.OK)
    fun `return merged flux`(): Unit {
        FluxService().gimmeMergedFlux().subscribe { thing -> println("Here is flux $thing") }
    }

    @GetMapping("/magic4")
    @ResponseStatus(HttpStatus.OK)
    fun `return flux`(): Unit {
        FluxService().gimmeFlux().subscribe { thing -> println("Here is flux [${thing.warnings}]") }
    }

    @GetMapping("/magic5")
    @ResponseStatus(HttpStatus.OK)
    fun `mono 1`(): Unit {
        val gimmeMono = MonoService().gimmeMono()
        println("got mono")

        gimmeMono.subscribe { thing -> println("Here is mono [${thing}]") }
    }
}

data class Result(
        val status: String,
        val passed: Boolean,
        val details: Map<String, String>,
        val warnings: Map<String, String>
)

data class CombinedResult(
        var status: String = "OKAY",
        var details: Map<String, String> = HashMap(),
        var warnings: Map<String, String> = HashMap()
)

class FluxService {
    fun gimmeFlux(): Flux<Result> = Flux.just(
            Result(
                    randomAlphabeticString(6),
                    randomBoolean(),
                    mapOf(
                            Pair(randomAlphabeticString(3), randomAlphabeticString(6)),
                            Pair(randomAlphabeticString(3), randomAlphabeticString(6))
                    ),
                    mapOf(
                            Pair(randomAlphabeticString(3), randomAlphabeticString(6)),
                            Pair(randomAlphabeticString(3), randomAlphabeticString(6))
                    )
            ))

    fun gimmeMergedFlux(): Flux<Result> {

        val allFluxes = Flux.merge(
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux()
        )
        println("merged")
        return allFluxes
    }
}

class MonoService {
    fun gimmeMono(): Mono<CombinedResult> {

        val allFluxes = Flux.merge(
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux()
        )
        println("merged")
        return allFluxes.reduce(CombinedResult(), { combinedResult, result -> addResults(combinedResult, result) })
    }

    fun gimmeMono2(): Mono<CombinedResult> {

        val allFluxes = Flux.merge(
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux(),
                FluxService().gimmeFlux()
        )
        println("merged")

        val combinedResult = CombinedResult()
        allFluxes.subscribe { result -> addResults(combinedResult = combinedResult, result = result) }
        println("subscribed")

        return Mono.just(combinedResult)
    }

    private fun addResults(combinedResult: CombinedResult, result: Result): CombinedResult {

        combinedResult.warnings = combinedResult.warnings.plus(result.warnings)
        combinedResult.details = combinedResult.details.plus(result.details)

        if (!result.passed)
            combinedResult.status = "FAIL"

        return combinedResult
    }
}