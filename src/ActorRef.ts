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
