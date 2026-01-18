package io.github.jckoenen

import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forAll
import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNot
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

class SqsFailureTest : FreeSpec({
    "${SqsFailure.UnknownFailure::class} must be assignable from all SqsFailure interfaces" {
        val subject = SqsFailure.UnknownFailure::class
        val interfaces = SqsFailure::class
            .sealedSubclasses
            .filter(KClass<out SqsFailure>::isSealed)

        interfaces shouldNot beEmpty()
        interfaces.forAll { subject.isSubclassOf(it) shouldBe true }
    }
})
