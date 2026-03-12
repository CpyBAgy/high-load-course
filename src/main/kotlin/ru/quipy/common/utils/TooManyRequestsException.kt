package ru.quipy.common.utils

class TooManyRequestsException(val retryAfter: Long) : RuntimeException()
