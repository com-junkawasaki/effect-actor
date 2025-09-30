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
