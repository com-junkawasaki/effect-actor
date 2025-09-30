// Metrics Implementation for Effect v3
// Merkle DAG Hash: metrics_v3_002
// Dependencies: ["effect/Metric"]

import { Effect } from "effect"
import { gauge, counter, set, increment } from "effect/Metric"
import type { ActorRef } from "./ActorRef"

/**
 * Actor System Metrics - Basic observability
 */
export interface ActorSystemMetrics {
  readonly actorCount: ReturnType<typeof gauge>
  readonly totalMessages: ReturnType<typeof counter>
  readonly failedMessages: ReturnType<typeof counter>
}

/**
 * Individual Actor Metrics
 */
export interface ActorMetrics<M> {
  readonly mailboxSize: ReturnType<typeof gauge>
  readonly processedMessages: ReturnType<typeof counter>
  readonly failures: ReturnType<typeof counter>
}

/**
 * Metrics Registry - Central metrics management
 */
export class MetricsRegistry {
  readonly _tag = "MetricsRegistry" as const

  constructor(
    readonly systemMetrics: ActorSystemMetrics,
    private readonly actorMetrics: Map<string, ActorMetrics<any>> = new Map()
  ) {}

  /**
   * Get or create metrics for an actor
   */
  getActorMetrics<M>(actorRef: ActorRef<M>): ActorMetrics<M> {
    const existing = this.actorMetrics.get(actorRef.id)
    if (existing) {
      return existing as ActorMetrics<M>
    }

    const metrics: ActorMetrics<M> = {
      mailboxSize: gauge(`actor_mailbox_size{actor="${actorRef.id}"}`),
      processedMessages: counter(`actor_messages_processed_total{actor="${actorRef.id}"}`),
      failures: counter(`actor_failures_total{actor="${actorRef.id}"}`)
    }

    this.actorMetrics.set(actorRef.id, metrics)
    return metrics
  }

  /**
   * Remove metrics for an actor
   */
  removeActorMetrics(actorRef: ActorRef<any>): Effect.Effect<void, never, never> {
    this.actorMetrics.delete(actorRef.id)
    return Effect.succeed(undefined)
  }

  /**
   * Update system metrics
   */
  updateSystemStats(
    actorCount: number
  ): Effect.Effect<void, never, never> {
    return set(this.systemMetrics.actorCount, actorCount).pipe(
      Effect.orDie
    )
  }
}

/**
 * Metrics Registry Factory
 */
export const MetricsRegistryUtils = {
  /**
   * Create a new metrics registry with system metrics
   */
  make(): Effect.Effect<MetricsRegistry, never, never> {
    return Effect.gen(function* () {
      const systemMetrics: ActorSystemMetrics = {
        actorCount: gauge("actor_system_actor_count"),
        totalMessages: counter("actor_system_messages_total"),
        failedMessages: counter("actor_system_messages_failed_total")
      }

      return new MetricsRegistry(systemMetrics)
    }).pipe(
      Effect.orDie
    )
  }
} as const

/**
 * Metrics Utilities - Helper functions for common metric operations
 */
export const MetricsUtils = {
  /**
   * Record message processing success
   */
  recordMessageProcessed<M>(
    systemMetrics: ActorSystemMetrics,
    actorMetrics: ActorMetrics<M>
  ): Effect.Effect<void, never, never> {
    return Effect.gen(function* () {
      yield* increment(systemMetrics.totalMessages)
      yield* increment(actorMetrics.processedMessages)
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Record message processing failure
   */
  recordMessageFailed<M>(
    systemMetrics: ActorSystemMetrics,
    actorMetrics: ActorMetrics<M>
  ): Effect.Effect<void, never, never> {
    return Effect.gen(function* () {
      yield* increment(systemMetrics.failedMessages)
      yield* increment(actorMetrics.failures)
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Update mailbox size
   */
  updateMailboxSize<M>(
    actorMetrics: ActorMetrics<M>,
    size: number
  ): Effect.Effect<void, never, never> {
    return set(actorMetrics.mailboxSize, size).pipe(
      Effect.orDie
    )
  },

  /**
   * Update actor count
   */
  updateActorCount(
    systemMetrics: ActorSystemMetrics,
    count: number
  ): Effect.Effect<void, never, never> {
    return set(systemMetrics.actorCount, count).pipe(
      Effect.orDie
    )
  }
} as const
