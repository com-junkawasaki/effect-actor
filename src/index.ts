// Effect Actors v3 - Main Exports
// Merkle DAG Hash: index_v3_001

// Core Types
export {
  ActorId,
  ActorStatus,
  SupervisionStrategy
} from "./types.ts"
export type {
  Envelope,
  ActorConfig,
  ActorContext,
  ActorBehavior,
  Message,
  TaggedMessage
} from "./types.ts"

// Actor Reference
export { ActorRef, ActorRefUtils } from "./ActorRef.ts"

// Message Protocol
export {
  messageMatcher,
  matchMessages,
  SystemMessageTag,
  SystemMessages,
  jsonMessageSerializer
} from "./Message.ts"
export type {
  MessageMatcher,
  StopMessage,
  RestartMessage,
  GetStatusMessage,
  MessageSerializer
} from "./Message.ts"

// Mailbox System
export { Mailbox, MailboxUtils } from "./Mailbox.ts"

// Actor Runtime
export { ActorRuntime, ActorRuntimeUtils } from "./ActorRuntime.ts"

// Supervision System
export { Supervisor, SupervisorTree, SupervisorUtils } from "./Supervisor.ts"

// Actor System
export { ActorSystem, ActorSystemUtils } from "./ActorSystem.ts"
export type { ActorSystemStats } from "./ActorSystem.ts"

// Re-export Effect types commonly used with actors
export type { Effect } from "effect"
