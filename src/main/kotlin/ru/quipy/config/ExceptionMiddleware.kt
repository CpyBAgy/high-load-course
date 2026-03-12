package ru.quipy.config

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.quipy.common.utils.TooManyRequestsException

@RestControllerAdvice
class ExceptionMiddleware {

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(e: TooManyRequestsException): ResponseEntity<Void> {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", e.retryAfter.toString())
            .build()
    }
}
