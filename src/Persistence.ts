// Persistence Layer for Effect Actors v3
// Merkle DAG Hash: persistence_v3_001
// Dependencies: ["effect/Schema", "effect/Stream"]

import { Effect, Schema, Stream } from "effect"

/**
 * Event Sourcing Persistence Layer
 * Provides durable actor state through event journaling
 */

// Event base schema
export interface PersistedEvent {
  readonly id: string
  readonly actorId: string
  readonly eventType: string
  readonly data: unknown
  readonly timestamp: number
  readonly version: number
}

// Event schema for validation
export const PersistedEventSchema = Schema.Struct({
  id: Schema.String,
  actorId: Schema.String,
  eventType: Schema.String,
  data: Schema.Unknown,
  timestamp: Schema.Number,
  version: Schema.Number
})

/**
 * Persistence Adapter Interface
 * Abstract interface for different persistence backends
 */
export interface PersistenceAdapter {
  readonly _tag: string

  /**
   * Save an event to the journal
   */
  saveEvent(event: PersistedEvent): Effect.Effect<void, PersistenceError, never>

  /**
   * Load events for an actor
   */
  loadEvents(actorId: string): Stream.Stream<PersistedEvent, PersistenceError, never>

  /**
   * Get the latest version for an actor
   */
  getLatestVersion(actorId: string): Effect.Effect<number, PersistenceError, never>

  /**
   * Close the persistence adapter
   */
  close(): Effect.Effect<void, never, never>
}

/**
 * Persistence Error Types
 */
export class PersistenceError {
  readonly _tag = "PersistenceError"

  constructor(
    readonly message: string,
    readonly cause?: Error
  ) {}
}

/**
 * In-Memory Persistence Adapter (for development/testing)
 */
export class InMemoryPersistenceAdapter implements PersistenceAdapter {
  readonly _tag = "InMemoryPersistenceAdapter"

  private events: Map<string, PersistedEvent[]> = new Map()

  saveEvent(event: PersistedEvent): Effect.Effect<void, PersistenceError, never> {
    return Effect.gen(function* () {
      const actorEvents = this.events.get(event.actorId) ?? []
      actorEvents.push(event)
      this.events.set(event.actorId, actorEvents)
      return undefined
    }).pipe(
      Effect.orDie
    )
  }

  loadEvents(actorId: string): Stream.Stream<PersistedEvent, PersistenceError, never> {
    const actorEvents = this.events.get(actorId) ?? []
    return Stream.fromIterable(actorEvents)
  }

  getLatestVersion(actorId: string): Effect.Effect<number, PersistenceError, never> {
    const actorEvents = this.events.get(actorId) ?? []
    const latest = actorEvents[actorEvents.length - 1]
    return Effect.succeed(latest?.version ?? 0)
  }

  close(): Effect.Effect<void, never, never> {
    return Effect.succeed(undefined)
  }
}

/**
 * Persistence Manager - Central persistence coordination
 */
export class PersistenceManager {
  readonly _tag = "PersistenceManager"

  constructor(
    private readonly adapter: PersistenceAdapter
  ) {}

  /**
   * Persist an actor event
   */
  persistEvent<M>(
    actorId: string,
    eventType: string,
    data: M
  ): Effect.Effect<PersistedEvent, PersistenceError, never> {
    return Effect.gen(function* () {
      const version = yield* this.adapter.getLatestVersion(actorId)
      const event: PersistedEvent = {
        id: crypto.randomUUID(),
        actorId,
        eventType,
        data,
        timestamp: Date.now(),
        version: version + 1
      }

      yield* this.adapter.saveEvent(event)
      return event
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Load actor events for state reconstruction
   */
  loadActorEvents(actorId: string): Stream.Stream<PersistedEvent, PersistenceError, never> {
    return this.adapter.loadEvents(actorId)
  }

  /**
   * Close the persistence manager
   */
  close(): Effect.Effect<void, never, never> {
    return this.adapter.close().pipe(
      Effect.orDie
    )
  }
}

/**
 * Persistence Manager Factory
 */
export const PersistenceManagerUtils = {
  /**
   * Create an in-memory persistence manager (for development)
   */
  inMemory(): Effect.Effect<PersistenceManager, never, never> {
    return Effect.gen(function* () {
      const adapter = new InMemoryPersistenceAdapter()
      return new PersistenceManager(adapter)
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Create persistence manager with custom adapter
   */
  withAdapter(adapter: PersistenceAdapter): Effect.Effect<PersistenceManager, never, never> {
    return Effect.succeed(new PersistenceManager(adapter))
  }
} as const
