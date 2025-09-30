// Chat Room Actor Example
// Demonstrates actor communication and state management

import { Effect } from "effect"
import {
  ActorSystemUtils,
  type ActorBehavior,
  type ActorRef,
  type Message
} from "../src/index.ts"

// Chat messages
interface JoinRoom extends Message {
  readonly _tag: "JoinRoom"
  readonly userId: string
  readonly username: string
}

interface LeaveRoom extends Message {
  readonly _tag: "LeaveRoom"
  readonly userId: string
}

interface SendMessage extends Message {
  readonly _tag: "SendMessage"
  readonly userId: string
  readonly content: string
  readonly timestamp: number
}

interface GetUsers extends Message {
  readonly _tag: "GetUsers"
}

interface GetHistory extends Message {
  readonly _tag: "GetHistory"
}

type ChatMessage = JoinRoom | LeaveRoom | SendMessage | GetUsers | GetHistory

// Chat state
interface User {
  readonly id: string
  readonly username: string
  readonly joinedAt: number
}

interface ChatMessage {
  readonly userId: string
  readonly username: string
  readonly content: string
  readonly timestamp: number
}

interface ChatRoomState {
  readonly users: Map<string, User>
  readonly messages: ChatMessage[]
  readonly maxMessages: number
}

// Message constructors
const ChatMessages = {
  joinRoom: (userId: string, username: string): JoinRoom => ({
    _tag: "JoinRoom",
    userId,
    username
  }),

  leaveRoom: (userId: string): LeaveRoom => ({
    _tag: "LeaveRoom",
    userId
  }),

  sendMessage: (userId: string, content: string): SendMessage => ({
    _tag: "SendMessage",
    userId,
    content,
    timestamp: Date.now()
  }),

  getUsers: (): GetUsers => ({
    _tag: "GetUsers"
  }),

  getHistory: (): GetHistory => ({
    _tag: "GetHistory"
  })
}

// Chat room actor behavior
const chatRoomBehavior: ActorBehavior<ChatRoomState, ChatMessage> = (state, message, context) => {
  return Effect.gen(function* () {
    switch (message._tag) {
      case "JoinRoom": {
        const user: User = {
          id: message.userId,
          username: message.username,
          joinedAt: Date.now()
        }

        const newUsers = new Map(state.users)
        newUsers.set(message.userId, user)

        const joinMessage: ChatMessage = {
          userId: message.userId,
          username: message.username,
          content: `${message.username} joined the room`,
          timestamp: Date.now()
        }

        const newMessages = [...state.messages, joinMessage].slice(-state.maxMessages)

        console.log(`User ${message.username} joined the chat room`)
        return { ...state, users: newUsers, messages: newMessages }
      }

      case "LeaveRoom": {
        const user = state.users.get(message.userId)
        if (!user) return state

        const newUsers = new Map(state.users)
        newUsers.delete(message.userId)

        const leaveMessage: ChatMessage = {
          userId: message.userId,
          username: user.username,
          content: `${user.username} left the room`,
          timestamp: Date.now()
        }

        const newMessages = [...state.messages, leaveMessage].slice(-state.maxMessages)

        console.log(`User ${user.username} left the chat room`)
        return { ...state, users: newUsers, messages: newMessages }
      }

      case "SendMessage": {
        const user = state.users.get(message.userId)
        if (!user) return state

        const chatMessage: ChatMessage = {
          userId: message.userId,
          username: user.username,
          content: message.content,
          timestamp: message.timestamp
        }

        const newMessages = [...state.messages, chatMessage].slice(-state.maxMessages)

        console.log(`[${user.username}]: ${message.content}`)
        return { ...state, messages: newMessages }
      }

      case "GetUsers": {
        const userList = Array.from(state.users.values()).map(u => u.username)
        console.log(`Current users: ${userList.join(", ")}`)
        return state
      }

      case "GetHistory": {
        console.log("=== Chat History ===")
        state.messages.forEach(msg => {
          console.log(`[${new Date(msg.timestamp).toLocaleTimeString()}] ${msg.username}: ${msg.content}`)
        })
        console.log("=== End History ===")
        return state
      }

      default:
        return state
    }
  })
}

// User actor - simulates a chat user
const createUserActor = (
  system: any, // ActorSystem
  chatRoom: ActorRef<ChatMessage>,
  userId: string,
  username: string
) => {
  interface UserState {
    readonly userId: string
    readonly username: string
    readonly messageCount: number
  }

  const userBehavior: ActorBehavior<UserState, string> = (state, message, context) => {
    return Effect.gen(function* () {
      // Send message to chat room
      yield* chatRoom.tell(ChatMessages.sendMessage(state.userId, message))

      return { ...state, messageCount: state.messageCount + 1 }
    })
  }

  return Effect.gen(function* () {
    const userActor = yield* system.make(
      `user-${userId}`,
      { userId, username, messageCount: 0 },
      userBehavior
    )

    // Join the chat room
    yield* chatRoom.tell(ChatMessages.joinRoom(userId, username))

    return userActor
  })
}

// Example usage
const runChatRoomExample = Effect.gen(function* () {
  console.log("=== Chat Room Actor Example ===")

  // Create actor system
  const system = yield* ActorSystemUtils.make("chat-system")

  // Create chat room
  const chatRoom = yield* system.make(
    "main-room",
    {
      users: new Map(),
      messages: [],
      maxMessages: 50
    },
    chatRoomBehavior,
    { mailboxCapacity: 1000 }
  )

  // Create some users
  const alice = yield* createUserActor(system, chatRoom, "alice", "Alice")
  const bob = yield* createUserActor(system, chatRoom, "bob", "Bob")
  const charlie = yield* createUserActor(system, chatRoom, "charlie", "Charlie")

  // Users send messages
  yield* alice.tell("Hello everyone!")
  yield* bob.tell("Hi Alice!")
  yield* charlie.tell("Good to see you all here")
  yield* alice.tell("How's the weather?")
  yield* bob.tell("It's sunny today!")

  // Wait for messages to be processed
  yield* Effect.sleep(200)

  // Get room info
  yield* chatRoom.tell(ChatMessages.getUsers())
  yield* chatRoom.tell(ChatMessages.getHistory())

  // Users leave
  yield* charlie.tell(ChatMessages.leaveRoom("charlie"))
  yield* bob.tell(ChatMessages.leaveRoom("bob"))

  yield* Effect.sleep(100)

  yield* chatRoom.tell(ChatMessages.getUsers())

  // Shutdown system
  yield* system.shutdown()
  console.log("Chat room example completed")
})

// Run the example
Effect.runPromise(runChatRoomExample).then(() => {
  console.log("Chat room example finished successfully")
}).catch(error => {
  console.error("Chat room example failed:", error)
})
