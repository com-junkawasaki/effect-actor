// Scope and Finalizer Support for Effect Actors v3
// Merkle DAG Hash: scope_v3_001
// Dependencies: ["effect/Scope", "effect/Ref"]

import { Effect, Scope, Ref } from "effect"

/**
 * Resource Management for Actors
 * Provides Scope-based resource management with automatic cleanup
 */

// Resource descriptor
export interface ManagedResource {
  readonly id: string
  readonly acquire: Effect.Effect<unknown, Error, never>
  readonly release: (resource: unknown) => Effect.Effect<void, never, never>
}

// Resource registry for an actor
export class ResourceRegistry {
  readonly _tag = "ResourceRegistry"

  constructor(
    private readonly resources: Map<string, ManagedResource & { instance?: unknown }> = new Map()
  ) {}

  /**
   * Register a resource with the actor
   */
  registerResource(resource: ManagedResource): Effect.Effect<string, never, never> {
    return Effect.gen(function* () {
      this.resources.set(resource.id, resource)
      return resource.id
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Acquire a resource
   */
  acquireResource(id: string): Effect.Effect<unknown, Error, Scope.Scope> {
    return Effect.gen(function* () {
      const resource = this.resources.get(id)

      if (!resource) {
        throw new Error(`Resource ${id} not found`)
      }

      const instance = yield* Effect.acquireRelease(
        resource.acquire,
        (res) => resource.release(res)
      )

      // Store the instance
      this.resources.set(id, { ...resource, instance })

      return instance
    })
  }

  /**
   * Release a specific resource
   */
  releaseResource(id: string): Effect.Effect<void, never, never> {
    return Effect.gen(function* () {
      const resource = this.resources.get(id)

      if (resource?.instance) {
        yield* resource.release(resource.instance)
        this.resources.delete(id)
      }
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Release all resources
   */
  releaseAll(): Effect.Effect<void, never, never> {
    return Effect.gen(function* () {
      for (const [id, resource] of this.resources) {
        if (resource.instance) {
          yield* resource.release(resource.instance)
        }
      }

      this.resources.clear()
    }).pipe(
      Effect.orDie
    )
  }
}

/**
 * Actor Scope Context - Provides resource management within actor context
 */
export interface ActorScopeContext {
  readonly resourceRegistry: ResourceRegistry
  readonly scope: Scope.Scope
}

/**
 * Resource Management Utilities
 */
export const ResourceUtils = {
  /**
   * Create a database connection resource
   */
  databaseConnection(url: string): ManagedResource {
    return {
      id: `db-${crypto.randomUUID()}`,
      acquire: Effect.gen(function* () {
        // Simulate database connection
        yield* Effect.log(`Acquiring database connection to ${url}`)
        return { url, connected: true }
      }),
      release: (conn) => Effect.gen(function* () {
        yield* Effect.log(`Releasing database connection to ${(conn as any).url}`)
        // Cleanup logic here
        return undefined
      })
    }
  },

  /**
   * Create a file handle resource
   */
  fileHandle(path: string): ManagedResource {
    return {
      id: `file-${crypto.randomUUID()}`,
      acquire: Effect.gen(function* () {
        yield* Effect.log(`Opening file ${path}`)
        return { path, opened: true }
      }),
      release: (handle) => Effect.gen(function* () {
        yield* Effect.log(`Closing file ${(handle as any).path}`)
        // Cleanup logic here
        return undefined
      })
    }
  },

  /**
   * Create a network connection resource
   */
  networkConnection(host: string, port: number): ManagedResource {
    return {
      id: `net-${crypto.randomUUID()}`,
      acquire: Effect.gen(function* () {
        yield* Effect.log(`Connecting to ${host}:${port}`)
        return { host, port, connected: true }
      }),
      release: (conn) => Effect.gen(function* () {
        yield* Effect.log(`Disconnecting from ${(conn as any).host}:${(conn as any).port}`)
        // Cleanup logic here
        return undefined
      })
    }
  }
} as const

/**
 * Scope Utilities for Actors
 */
export const ScopeUtils = {
  /**
   * Execute an effect within an actor scope with resource management
   */
  withActorScope<R, E, A>(
    effect: (context: ActorScopeContext) => Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R | Scope.Scope> {
    return Effect.gen(function* () {
      const scope = yield* Scope.make()
      const resourceRegistry = new ResourceRegistry()

      const context: ActorScopeContext = {
        resourceRegistry,
        scope
      }

      try {
        const result = yield* effect(context)
        return result
      } finally {
        // Ensure resources are cleaned up
        yield* resourceRegistry.releaseAll()
      }
    })
  },

  /**
   * Create a scoped resource operation
   */
  scopedResource<T>(
    resource: ManagedResource,
    operation: (resource: unknown) => Effect.Effect<T, Error, Scope.Scope>
  ): Effect.Effect<T, Error, Scope.Scope> {
    return Effect.gen(function* () {
      const scope = yield* Scope.make()
      const registry = new ResourceRegistry()

      yield* registry.registerResource(resource)
      const instance = yield* registry.acquireResource(resource.id)

      try {
        const result = yield* operation(instance)
        return result
      } finally {
        yield* registry.releaseResource(resource.id)
      }
    })
  }
} as const
