// Copyright 2025 Effect Community
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Core Types for Effect Actors v3
// Merkle DAG Hash: types_v3_001

import type { Effect } from "effect"

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
 * Contains references to self and system services
 */
export interface ActorContext<M> {
  readonly self: ActorRef<M>
  readonly system: ActorSystem
  readonly config: ActorConfig
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
