// Actor Reference Implementation for Effect v3
// Merkle DAG Hash: actor_ref_v3_001
// Dependencies: []

import type { Effect } from "effect"
import type { ActorId, Envelope } from "./types.ts"

/**
 * ActorRef - Reference to an actor for message sending
 * Provides type-safe message delivery interface
 */
export class ActorRef<M> {
  readonly _tag = "ActorRef" as const

  constructor(
    readonly id: ActorId,
    readonly uri: string,
    private readonly sendMessage: (envelope: Envelope<M>) => Effect.Effect<void, Error, never>
  ) {}

  /**
   * Send a message to this actor
   * Asynchronous fire-and-forget delivery
   */
  tell(message: M): Effect.Effect<void, Error, never> {
    const envelope: Envelope<M> = {
      message,
      timestamp: Date.now()
    }
    return this.sendMessage(envelope)
  }

  /**
   * Send a message to this actor with sender context
   */
  tellFrom<S>(message: M, sender: ActorRef<S>): Effect.Effect<void, Error, never> {
    const envelope: Envelope<M> = {
      message,
      sender,
      timestamp: Date.now()
    }
    return this.sendMessage(envelope)
  }

  /**
   * Get string representation of this actor reference
   */
  toString(): string {
    return `ActorRef(${this.uri})`
  }

  /**
   * Check equality with another actor reference
   */
  equals(other: ActorRef<unknown>): boolean {
    return this.id === other.id && this.uri === other.uri
  }
}

/**
 * Utility functions for ActorRef
 */
export const ActorRefUtils = {
  /**
   * Create a new ActorId from string
   */
  makeId(id: string): ActorId {
    return id as ActorId
  },

  /**
   * Check if value is an ActorRef
   */
  isActorRef(value: unknown): value is ActorRef<unknown> {
    return value instanceof ActorRef && value._tag === "ActorRef"
  }
} as const
