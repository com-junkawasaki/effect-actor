// Copyright (c) 2025 Effect Community
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Core Types for Effect Actors v3
// Merkle DAG Hash: types_v3_002

import type { Effect } from "effect"
// import type { PersistenceManager } from "./Persistence"
// import type { ActorScopeContext } from "./Scope"
// import type { RetryConfig, RateLimitConfig } from "./Retry"

// Forward declarations to avoid circular imports
export type ActorRef<M> = any // Will be defined in ActorRef.ts
export type ActorSystem = any // Will be defined in ActorSystem.ts

/**
 * Actor ID - Unique identifier for actors
 * Ensures global uniqueness across the actor system
 */
export type ActorId = string & { readonly _tag: "ActorId" }

/**
 * Message envelope for type-safe messaging
 * Provides sender context and message payload
 */
export interface Envelope<M> {
  readonly message: M
  readonly sender?: ActorRef<unknown>
  readonly timestamp: number
}

/**
 * Actor status enumeration
 * Defines the lifecycle states of an actor
 */
export const ActorStatus = {
  Starting: "Starting",
  Running: "Running",
  Stopping: "Stopping",
  Stopped: "Stopped",
  Failed: "Failed"
} as const

export type ActorStatus = typeof ActorStatus[keyof typeof ActorStatus]

/**
 * Actor configuration
 * Defines actor behavior and limits
 */
export interface ActorConfig {
  readonly mailboxCapacity?: number
  readonly maxRestarts?: number
  readonly restartDelay?: number
  readonly supervisionStrategy?: SupervisionStrategy
  readonly messageTimeout?: number // in milliseconds

  // NOTE: Advanced features will be added in future versions
  // readonly persistenceEnabled?: boolean
  // readonly persistence?: PersistenceManager
  // readonly scopeEnabled?: boolean
  // readonly retryConfig?: RetryConfig
  // readonly rateLimitConfig?: RateLimitConfig
}

/**
 * Supervision strategy for actor failure recovery
 */
export const SupervisionStrategy = {
  Stop: "Stop",
  Restart: "Restart",
  Escalate: "Escalate"
} as const

export type SupervisionStrategy = typeof SupervisionStrategy[keyof typeof SupervisionStrategy]

/**
 * Actor context provided to actor behavior functions
 * Contains references to self, system services, and advanced features
 */
export interface ActorContext<M> {
  readonly self: ActorRef<M>
  readonly system: ActorSystem
  readonly config: ActorConfig

  // NOTE: Advanced features will be added in future versions
  // readonly persistence?: PersistenceManager
  // readonly scope?: ActorScopeContext
  // readonly retryConfig?: RetryConfig
  // readonly rateLimitConfig?: RateLimitConfig
}

/**
 * Actor behavior function type
 * Defines how an actor processes messages and maintains state
 */
export type ActorBehavior<S, M> = (
  state: S,
  message: M,
  context: ActorContext<M>
) => Effect.Effect<S, Error, never>

/**
 * Base message interface
 * All actor messages should extend this
 */
export interface Message {
  readonly _tag: string
}

/**
 * Tagged message type helper
 * Creates nominal typing for message types
 */
export type TaggedMessage<T extends string> = Message & {
  readonly _tag: T
}
