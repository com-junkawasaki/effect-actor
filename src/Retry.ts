// Retry and Rate Limit Support for Effect Actors v3
// Merkle DAG Hash: retry_v3_001
// Dependencies: ["effect/Schedule", "effect/Clock"]

import { Effect, Schedule, Clock } from "effect"

/**
 * Retry and Rate Limiting for Actor Operations
 * Provides resilient operation execution with backoff and rate control
 */

// Retry configuration
export interface RetryConfig {
  readonly maxAttempts: number
  readonly initialDelayMs: number
  readonly maxDelayMs: number
  readonly backoffFactor: number
}

// Rate limit configuration
export interface RateLimitConfig {
  readonly requestsPerSecond: number
  readonly burstSize: number
}

// Default configurations
export const RetryDefaults = {
  config: {
    maxAttempts: 3,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    backoffFactor: 2.0
  } as RetryConfig
} as const

export const RateLimitDefaults = {
  config: {
    requestsPerSecond: 10,
    burstSize: 5
  } as RateLimitConfig
} as const

/**
 * Rate Limiter - Controls operation frequency
 */
export class RateLimiter {
  private lastRequestTime: number = 0
  private requestCount: number = 0
  private readonly intervalMs: number

  constructor(
    private readonly config: RateLimitConfig
  ) {
    this.intervalMs = 1000 / config.requestsPerSecond
  }

  /**
   * Check if operation can proceed
   */
  canProceed(): boolean {
    const now = Date.now()
    const timeSinceLastRequest = now - this.lastRequestTime

    // Reset counter if interval has passed
    if (timeSinceLastRequest >= this.intervalMs) {
      this.requestCount = 0
      this.lastRequestTime = now
      return true
    }

    // Check burst capacity
    if (this.requestCount < this.config.burstSize) {
      this.requestCount++
      return true
    }

    return false
  }

  /**
   * Wait until operation can proceed
   */
  waitForSlot(): Effect.Effect<void, never, never> {
    return Effect.gen(function* () {
      while (!this.canProceed()) {
        yield* Effect.sleep(100) // Check every 100ms
      }
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get current rate limit status
   */
  getStatus(): { canProceed: boolean; requestCount: number; lastRequestTime: number } {
    return {
      canProceed: this.canProceed(),
      requestCount: this.requestCount,
      lastRequestTime: this.lastRequestTime
    }
  }
}

/**
 * Retry Utilities - Handle transient failures
 */
export const RetryUtils = {
  /**
   * Create exponential backoff schedule
   */
  exponentialBackoff(config: RetryConfig): Schedule.Schedule<number, Error, never> {
    return Schedule.exponential(config.initialDelayMs).pipe(
      Schedule.compose(Schedule.elapsed),
      Schedule.whileInput((n) => n < config.maxDelayMs),
      Schedule.compose(Schedule.recurs(config.maxAttempts - 1))
    )
  },

  /**
   * Retry an operation with exponential backoff
   */
  retryWithBackoff<A, E, R>(
    effect: Effect.Effect<A, E, R>,
    config: RetryConfig = RetryDefaults.config
  ): Effect.Effect<A, E, R> {
    const schedule = this.exponentialBackoff(config)
    return Effect.retry(effect, schedule)
  },

  /**
   * Retry with custom predicate
   */
  retryWithPredicate<A, E, R>(
    effect: Effect.Effect<A, E, R>,
    predicate: (error: E) => boolean,
    config: RetryConfig = RetryDefaults.config
  ): Effect.Effect<A, E, R> {
    const schedule = this.exponentialBackoff(config)
    return Effect.retry(effect, Schedule.whileInput(predicate), { schedule })
  },

  /**
   * Retry with circuit breaker pattern
   */
  withCircuitBreaker<A, E, R>(
    effect: Effect.Effect<A, E, R>,
    failureThreshold: number = 5,
    recoveryTimeoutMs: number = 60000
  ): Effect.Effect<A, E, R> {
    // Simplified circuit breaker implementation
    // In production, use a proper circuit breaker library
    const config = { ...RetryDefaults.config, maxAttempts: failureThreshold }
    return RetryUtils.retryWithBackoff(effect, config)
  }
} as const

/**
 * Rate Limit Utilities
 */
export const RateLimitUtils = {
  /**
   * Create a rate limiter
   */
  make(config: RateLimitConfig = RateLimitDefaults.config): RateLimiter {
    return new RateLimiter(config)
  },

  /**
   * Execute operation with rate limiting
   */
  withRateLimit<A, E, R>(
    effect: Effect.Effect<A, E, R>,
    limiter: RateLimiter
  ): Effect.Effect<A, E, R> {
    return Effect.gen(function* () {
      yield* limiter.waitForSlot()
      return yield* effect
    })
  },

  /**
   * Create rate-limited operation
   */
  rateLimited<A, E, R>(
    operation: () => Effect.Effect<A, E, R>,
    config: RateLimitConfig = RateLimitDefaults.config
  ): (() => Effect.Effect<A, E, R>) {
    const limiter = this.make(config)
    return () => this.withRateLimit(operation(), limiter)
  }
} as const

/**
 * Resilience Utilities - Combine retry and rate limiting
 */
export const ResilienceUtils = {
  /**
   * Execute operation with full resilience (retry + rate limit)
   */
  resilientOperation<A, E, R>(
    effect: Effect.Effect<A, E, R>,
    retryConfig: RetryConfig = RetryDefaults.config,
    rateLimitConfig: RateLimitConfig = RateLimitDefaults.config
  ): Effect.Effect<A, E, R> {
    const limiter = RateLimitUtils.make(rateLimitConfig)

    return Effect.gen(function* () {
      yield* limiter.waitForSlot()
      return yield* RetryUtils.retryWithBackoff(effect, retryConfig)
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Create resilient actor behavior wrapper
   */
  resilientActorBehavior<S, M>(
    behavior: (state: S, message: M, context: any) => Effect.Effect<S, Error, never>,
    retryConfig?: RetryConfig,
    rateLimitConfig?: RateLimitConfig
  ): (state: S, message: M, context: any) => Effect.Effect<S, Error, never> {
    return (state: S, message: M, context: any) => {
      const effect = behavior(state, message, context)
      return ResilienceUtils.resilientOperation(effect, retryConfig, rateLimitConfig)
    }
  }
} as const
