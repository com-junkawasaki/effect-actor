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
