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

// Effect Actors v3 - Main Exports
// Merkle DAG Hash: index_v3_001

// Core Types
export {
  ActorId,
  ActorStatus,
  SupervisionStrategy
} from "./types"
export type {
  Envelope,
  ActorConfig,
  ActorContext,
  ActorBehavior,
  Message,
  TaggedMessage
} from "./types"

// Actor Reference
export { ActorRef, ActorRefUtils } from "./ActorRef"

// Message Protocol
export {
  messageMatcher,
  matchMessages,
  SystemMessageTag,
  SystemMessages,
  jsonMessageSerializer
} from "./Message"
export type {
  MessageMatcher,
  StopMessage,
  RestartMessage,
  GetStatusMessage,
  MessageSerializer
} from "./Message"

// Mailbox System
export { Mailbox, MailboxUtils } from "./Mailbox"

// Actor Runtime
export { ActorRuntime, ActorRuntimeUtils } from "./ActorRuntime"

// Supervision System
export { Supervisor, SupervisorTree, SupervisorUtils } from "./Supervisor"

// Actor System
export { ActorSystem, ActorSystemUtils } from "./ActorSystem"
export type { ActorSystemStats } from "./ActorSystem"

// Re-export Effect types commonly used with actors
export type { Effect } from "effect"
