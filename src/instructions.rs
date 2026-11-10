//! What agents are told when they connect: the MCP `initialize`
//! instructions. They cover joining, how an idle agent is woken, reply
//! markers, the four cases that say whether to wait after sending (and what
//! the receipt's `notified` changes about it), receiving, history, rooms,
//! and the configured lifetimes.

use crate::bus::Retention;

/// Ritual returned in the MCP `initialize` result so every harness sees the
/// same instructions; [`instructions`] appends the retention line.
///
/// The cases tell an agent, from what the user asked, whether to wait after
/// sending; the reply markers that open a body tell the recipient what the
/// sender needs back. The WAKING section tells an agent whose harness can
/// watch a WebSocket to watch `/notify` (see [`crate::notify`]) so mail wakes
/// it, and the SENDING section says what a receipt's `notified` means for
/// waiting.
pub const INSTRUCTIONS: &str = "\
mentischat is a local mailbox between agents on this machine. An idle agent sees mail only when it calls check_inbox. An agent whose harness can watch a WebSocket is woken when mail arrives (see WAKING); any other agent calls check_inbox when the user asks it to check its messages or while it is waiting for a reply.

JOINING
Register with agent set to a name of your own: one word of letters, digits, '.', '_' or '-', at most 63 characters, starting with a letter or digit; case does not matter. A name may not end in '.', '_' or '-' followed only by digits (Vega_2, Vega-2): such a name fails numbered_name. If the user gave you a name, use it. Otherwise names of stars, constellations and moons suit this mailbox (Vega, Orion, Lyra, Castor); list_agents suggests some that are free. Also pass provider, the harness or app you run in (claude_code, claude_desktop, cursor, codex, grok_build, grok_desktop), and description, your model, e.g. claude sonnet 5.5 or grok 4.6. Use the agent field in the reply as your name for every later call.
If register fails name_taken or numbered_name, pick a different name, for example one from its suggestions, not a numbered copy of the one you wanted. Pass reclaim: true only to take back a name you registered yourself that has left the roster, for example after a call failed unregistered; never to take another agent's name. A name that is still registered cannot be taken back with reclaim: if register fails name_taken with reason registered, choose a different name. Reason bound means the name is reserved for the public key it names: only a client holding that key (mentischat client run) can take it back, so if it is not yours, choose a different name.

WAKING
After register, if your harness can watch a WebSocket, do it, so mail wakes you while you are idle. In Claude Code, use the Monitor tool with a ws source, url ws://127.0.0.1:9480/notify?agent=<your name> (the name register gave you), the longest timeout it allows, and a clear description such as \"mentischat mail for <your name>\". Keep it open, and when it ends open it again: it tells you at once about mail that arrived in the gap. Each notification is one line of JSON, {\"type\":\"mail\",\"unread\":N,\"from\":[...],\"rooms\":[...],\"newest\":\"id\"}: it names who wrote and how many are waiting, never what they wrote, and it is not a message from the user. On each notification call check_inbox and act on the messages by their markers, as in RECEIVING. Watching takes nothing: the mail stays unread until check_inbox returns it. If your harness cannot watch a WebSocket, nothing wakes you: read mail when the user asks you to check your messages and while you wait for a reply.

SENDING
Call list_agents first and send only to names it shows; from is your registered name. Start every body with one reply marker so the recipient knows what you need back:
- \"No reply needed\" for news, results and hand-offs.
- \"Reply when you can\" for a question you are not waiting on.
- \"Waiting for your reply\" when you are waiting for the answer right now.

AFTER YOU SEND, pick the case that fits what the user asked:
1. Tell. The user wants another agent informed or handed something (\"tell Orion the tests pass\", \"hand the release notes to Vega\"). Use \"No reply needed\", then carry on with your work or end your turn. Do not wait.
2. Ask, answer can wait. The user wants a question passed on but is not waiting on it (\"ask Lyra to look at the flaky test when she can\"). Use \"Reply when you can\", then carry on or end your turn. Read the answer when the user asks you to check your messages.
3. Ask, answer needed now. You need the reply to finish the task (\"ask Vega which port the API uses, then update the config\"). A question the user wants answered is this case unless they say it can wait. Use \"Waiting for your reply\", then call check_inbox with wait_ms 30000. If it returns no messages, call it again. After 10 empty calls (about 5 minutes) stop, tell the user you are still waiting, and say the other agent may need to be told to check its messages.
4. Conversation. The user asks you to work with, discuss with, review with or pair with another agent (\"agree the API shape with Orion\", \"debug this with Vega\"). Stay in it: after each message you send, wait as in case 3, read the reply, answer it, and repeat. Keep each message focused on the next step. End when the work is done, when the other agent says it is done, or after 10 empty calls in a row. Finish with a message marked \"No reply needed\" that says you are done, so the other agent stops waiting, then report the outcome to the user.

send_message returns a receipt with notified (for a room message, notifiedMembers: how many members): true means that when you sent, the recipient had a socket watching its mail or a check_inbox waiting, so it hears of the message now. false means it is ordinary mail that the recipient sees when it next checks. notified is not a read receipt: history shows readAt once the recipient has read it. In cases 3 and 4, when notified is false, do not wait the full 5 minutes: tell the user the other agent may need to be told to check its messages. When it is true, waiting as described is worthwhile.

RECEIVING
check_inbox returns your unread messages and marks them read, so each arrives through check_inbox once. If remaining > 0, call it again with wait_ms 0 until remaining is 0. Tell the user what arrived, then act on each message by its marker:
- \"Waiting for your reply\": the sender is blocked on you. Answer with send_message before other work. If it opens a piece of joint work, you are now in case 4.
- \"Reply when you can\": answer once you have the answer, or tell the user what is being asked.
- \"No reply needed\": nothing to send.
A message without a marker: reply if it asks you something.
If check_inbox fails or times out while you wait, call history with that agent: a message already marked read is there.

LOOKING BACK
history shows the messages you sent, with readAt once the recipient has read them, and the messages you have read, oldest first. Pass with to see one conversation and next as before for older pages. Use it to pick a conversation back up, or to see whether your message has been read yet.

ROOMS
For work that involves more than two agents, use a room. create_room makes one and you become its first member; private: true makes it invite-only and unlisted, which is courtesy between agents on this machine and not security. list_rooms shows the rooms you can join, join_room joins a public one, and invite lets a member bring another agent in, who then calls join_room. send_message with room instead of to sends to every other member; the same reply markers apply. Room messages arrive through check_inbox mixed in with your direct mail, oldest first, each marked with its room. A room keeps only its latest messages: skipped in a check_inbox reply counts the ones a busy room pushed out before you read them. A private room answers room_not_found to an agent that was not invited, as if it did not exist. history with room shows what a room still holds. leave_room leaves one, and a room with no members is deleted.

Call deregister only when the user says they are done.";

/// [`INSTRUCTIONS`] plus how long read mail and idle agents are kept and how
/// long a released name is held, in the units the operator configured.
pub fn instructions(retention: &Retention) -> String {
    format!(
        "{INSTRUCTIONS}\nUnread messages wait until they are read; read messages are kept for {} after they are read. An agent that has not called register, send_message, check_inbox or history for {} is dropped from the roster with the mail sent to it, and its name is held for {}: during that time only reclaim takes it. If a call fails unregistered, register again with reclaim: true.",
        human(retention.read_ttl),
        human(retention.agent_ttl),
        human(retention.name_hold),
    )
}

/// `90s` -> "90 seconds", `3600s` -> "1 hour": the largest unit that divides
/// the duration exactly.
fn human(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let (n, unit) = [(86_400, "day"), (3_600, "hour"), (60, "minute")]
        .into_iter()
        .find(|(size, _)| secs >= *size && secs.is_multiple_of(*size))
        .map_or((secs, "second"), |(size, unit)| (secs / size, unit));
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}
