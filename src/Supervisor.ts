// Supervision System Implementation for Effect v3
// Merkle DAG Hash: supervision_v3_001
// Dependencies: ["actor_ref", "actor_runtime"]

import { Effect, Ref } from "effect"
import { SupervisionStrategy } from "./types.ts"
import type { ActorRef } from "./ActorRef.ts"
import type { ActorRuntime } from "./ActorRuntime.ts"

/**
 * Supervisor - Manages actor failure recovery
 * Implements various supervision strategies for resilient actor systems
 */
export class Supervisor<M> {
  readonly _tag = "Supervisor" as const

  constructor(
    readonly supervisedActor: ActorRef<M>,
    readonly strategy: SupervisionStrategy,
    readonly maxRestarts: number,
    readonly restartDelay: number,
    private readonly restartCountRef: Ref.Ref<number>,
    private readonly runtimeRef: Ref.Ref<ActorRuntime<unknown, M> | null>
  ) {}

  /**
   * Handle actor failure
   * Applies supervision strategy based on failure type
   */
  handleFailure(
    error: Error,
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, Error, never> {
    const self = this
    return Effect.gen(function* () {
      const currentRestarts = yield* Ref.get(self.restartCountRef)

      switch (self.strategy) {
        case SupervisionStrategy.Stop:
          yield* self.stopActor(runtime)
          break

        case SupervisionStrategy.Restart:
          if (currentRestarts < self.maxRestarts) {
            yield* self.restartActor(runtime)
            yield* Ref.set(self.restartCountRef, currentRestarts + 1)
          } else {
            yield* self.stopActor(runtime)
          }
          break

        case SupervisionStrategy.Escalate:
          // Escalate to parent supervisor (not implemented in this version)
          yield* self.stopActor(runtime)
          break

        default:
          yield* self.stopActor(runtime)
      }
    })
  }

  /**
   * Restart the supervised actor
   */
  private restartActor(
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, Error, never> {
    return Effect.gen(function* () {
      // Stop current runtime
      yield* runtime.stop()

      // Wait for restart delay
      if (this.restartDelay > 0) {
        yield* Effect.sleep(this.restartDelay)
      }

      // Get current runtime and restart it
      const currentRuntime = yield* Ref.get(this.runtimeRef)
      if (currentRuntime) {
        // TODO: Need to store initial state and behavior for restart
        // For now, just log that restart would happen
        yield* Effect.log(`Restarting actor ${this.supervisedActor.id}`)
      }
    })
  }

  /**
   * Stop the supervised actor
   */
  private stopActor(
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, Error, never> {
    return Effect.gen(function* () {
      yield* runtime.stop()
      yield* Effect.log(`Stopped actor ${this.supervisedActor.id} due to failure`)
    })
  }

  /**
   * Register a runtime with this supervisor
   */
  registerRuntime(runtime: ActorRuntime<unknown, M>): Effect.Effect<void, Error, never> {
    return Ref.set(this.runtimeRef, runtime)
  }

  /**
   * Get current restart count
   */
  getRestartCount(): Effect.Effect<number, Error, never> {
    return Ref.get(this.restartCountRef)
  }
}

/**
 * Supervisor factory and utilities
 */
export const SupervisorUtils = {
  /**
   * Create a supervisor with specified strategy
   */
  make<M>(
    actorRef: ActorRef<M>,
    strategy: SupervisionStrategy = SupervisionStrategy.Restart,
    maxRestarts: number = 3,
    restartDelay: number = 1000
  ): Effect.Effect<Supervisor<M>, Error, never> {
    return Effect.gen(function* () {
      const restartCountRef = yield* Ref.make(0)
      const runtimeRef = yield* Ref.make<ActorRuntime<unknown, M> | null>(null)

      return new Supervisor(
        actorRef,
        strategy,
        maxRestarts,
        restartDelay,
        restartCountRef,
        runtimeRef
      )
    })
  },

  /**
   * Create a supervisor tree for hierarchical supervision
   */
  tree<M>(
    supervisors: Supervisor<M>[]
  ): Effect.Effect<SupervisorTree<M>, Error, never> {
    return Effect.gen(function* () {
      const childrenRef = yield* Ref.make(supervisors)

      return new SupervisorTree(childrenRef)
    })
  }
} as const

/**
 * Supervisor Tree - Hierarchical supervision structure
 * Allows parent supervisors to manage child supervisors
 */
export class SupervisorTree<M> {
  readonly _tag = "SupervisorTree" as const

  constructor(
    private readonly childrenRef: Ref.Ref<Supervisor<M>[]>
  ) {}

  /**
   * Add a child supervisor
   */
  addChild(supervisor: Supervisor<M>): Effect.Effect<void, Error, never> {
    return Ref.update(this.childrenRef, children => [...children, supervisor])
  }

  /**
   * Remove a child supervisor
   */
  removeChild(actorRef: ActorRef<M>): Effect.Effect<void, Error, never> {
    return Ref.update(this.childrenRef, children =>
      children.filter(child => !child.supervisedActor.equals(actorRef))
    )
  }

  /**
   * Handle failure in the supervisor tree
   * Escalates failures up the hierarchy
   */
  handleFailure(
    error: Error,
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, Error, never> {
    return Effect.gen(function* () {
      const children = yield* Ref.get(this.childrenRef)
      const supervisor = children.find(child =>
        child.supervisedActor.equals(runtime.ref)
      )

      if (supervisor) {
        yield* supervisor.handleFailure(error, runtime)
      } else {
        // No supervisor found, escalate to root
        yield* Effect.log(`No supervisor found for actor ${runtime.ref.id}, escalating failure`)
        yield* Effect.fail(error)
      }
    })
  }

  /**
   * Get all child supervisors
   */
  getChildren(): Effect.Effect<Supervisor<M>[], Error, never> {
    return Ref.get(this.childrenRef)
  }
}
