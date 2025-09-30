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

// Actor System Implementation for Effect v3
// Merkle DAG Hash: actor_system_v3_001
// Dependencies: ["actor_ref", "mailbox", "actor_runtime", "supervision"]

import { Effect, Ref } from "effect"
import type { ActorBehavior, ActorConfig } from "./types"
import type { ActorId } from "./types"
import { ActorRef, ActorRefUtils } from "./ActorRef"
import { MailboxUtils } from "./Mailbox"
import type { ActorRuntime } from "./ActorRuntime"
import { SupervisorUtils } from "./Supervisor"
import { ActorRuntimeUtils } from "./ActorRuntime"
import { MetricsRegistryUtils } from "./Metrics"
import type { MetricsRegistry, ActorSystemMetrics } from "./Metrics"

/**
 * Actor System - Central coordinator for actor lifecycle management
 * Manages actor creation, supervision, and system-wide coordination
 */
export class ActorSystem {
  readonly _tag = "ActorSystem" as const

  constructor(
    readonly name: string,
    private readonly actorsRef: Ref.Ref<Map<ActorId, ActorRuntime<unknown, unknown>>>,
    private readonly supervisorsRef: Ref.Ref<Map<ActorId, any>>, // Supervisor<any>
    private readonly metricsRegistry: MetricsRegistry
  ) {}

  /**
   * Create a new actor with specified behavior
   */
  make<S, M>(
    id: string,
    initialState: S,
    behavior: ActorBehavior<S, M>,
    config: ActorConfig = {}
  ): Effect.Effect<ActorRef<M>, never, never> {
    const self = this
    return Effect.gen(function* () {
      const actorId = ActorRefUtils.makeId(id)
      const uri = `actor://${self.name}/${id}`

      // Check if actor already exists
      const existingActors = yield* Ref.get(self.actorsRef)
      if (existingActors.has(actorId)) {
        throw new Error(`Actor ${id} already exists`)
      }

      // Create mailbox
      const mailboxCapacity = config.mailboxCapacity ?? 1000
      const mailbox = yield* MailboxUtils.bounded<M>(mailboxCapacity)

      // Create actor reference
      const actorRef = new ActorRef<M>(
        actorId,
        uri,
        (envelope) => mailbox.offer(envelope)
      )

      // Get actor metrics
      const actorMetrics = self.metricsRegistry.getActorMetrics(actorRef)
      const systemMetrics = self.metricsRegistry.systemMetrics

      // Create runtime
      const runtime = yield* ActorRuntimeUtils.make<S, M>(actorRef, mailbox, config, systemMetrics, actorMetrics)

      // Create supervisor
      const supervisor = yield* SupervisorUtils.make(
        actorRef,
        initialState,
        behavior,
        config.supervisionStrategy,
        config.maxRestarts ?? 3,
        config.restartDelay ?? 1000
      )

      // Register supervisor
      yield* Ref.update(self.supervisorsRef, supervisors => {
        supervisors.set(actorId, supervisor)
        return supervisors
      })

      // Register runtime
      yield* Ref.update(self.actorsRef, actors => {
        actors.set(actorId, runtime as ActorRuntime<unknown, unknown>)
        return actors
      })

      // Register runtime with supervisor
      yield* supervisor.registerRuntime(runtime as ActorRuntime<unknown, M>)

      // Start the actor
      yield* runtime.start(initialState, behavior)

      return actorRef
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get an actor by ID
   */
  get<M>(id: string): Effect.Effect<ActorRef<M> | null, Error, never> {
    const self = this
    return Effect.gen(function* () {
      const actors = yield* Ref.get(self.actorsRef)
      const runtime = actors.get(ActorRefUtils.makeId(id))

      return runtime ? (runtime.ref as ActorRef<M>) : null
    })
  }

  /**
   * Stop an actor by ID
   */
  stop(id: string): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      const actorId = ActorRefUtils.makeId(id)
      const actors = yield* Ref.get(self.actorsRef)

      const runtime = actors.get(actorId)
      if (runtime) {
        yield* runtime.stop()

        // Remove from registry
        yield* Ref.update(self.actorsRef, actors => {
          actors.delete(actorId)
          return actors
        })

        // Remove supervisor
        yield* Ref.update(self.supervisorsRef, supervisors => {
          supervisors.delete(actorId)
          return supervisors
        })
      }
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get all running actors
   */
  getActors(): Effect.Effect<ActorRef<unknown>[], Error, never> {
    const self = this
    return Effect.gen(function* () {
      const actors = yield* Ref.get(self.actorsRef)

      const refs: ActorRef<unknown>[] = []
      for (const runtime of actors.values()) {
        refs.push(runtime.ref)
      }

      return refs
    })
  }

  /**
   * Shutdown the entire actor system
   */
  shutdown(): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      const actors = yield* Ref.get(self.actorsRef)

      // Stop all actors
      for (const runtime of actors.values()) {
        yield* runtime.stop()
      }

      // Clear registries
      yield* Ref.set(self.actorsRef, new Map())
      yield* Ref.set(self.supervisorsRef, new Map())

      yield* Effect.log(`Actor system ${self.name} shutdown complete`)
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get system statistics
   */
  getStats(): Effect.Effect<ActorSystemStats, never, never> {
    const self = this
    return Effect.gen(function* () {
      const actors = yield* Ref.get(self.actorsRef)
      const supervisors = yield* Ref.get(self.supervisorsRef)

      return {
        name: self.name,
        actorCount: actors.size,
        supervisorCount: supervisors.size,
        uptime: Date.now() // TODO: Track actual start time
      }
    })
  }
}

/**
 * Actor System Statistics
 */
export interface ActorSystemStats {
  readonly name: string
  readonly actorCount: number
  readonly supervisorCount: number
  readonly uptime: number
}

/**
 * Actor System factory and utilities
 */
export const ActorSystemUtils = {
  /**
   * Create a new actor system
   */
  make(name: string): Effect.Effect<ActorSystem, never, never> {
    return Effect.gen(function* () {
      const actorsRef = yield* Ref.make(new Map<ActorId, ActorRuntime<unknown, unknown>>())
      const supervisorsRef = yield* Ref.make(new Map<ActorId, any>())
      const metricsRegistry = yield* MetricsRegistryUtils.make()

      return new ActorSystem(name, actorsRef, supervisorsRef, metricsRegistry)
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Create a child actor system
   */
  child(parent: ActorSystem, name: string): Effect.Effect<ActorSystem, Error, never> {
    return ActorSystemUtils.make(`${parent.name}/${name}`)
  }
} as const
