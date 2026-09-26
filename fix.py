def build_pass1_prompt(episode_text: str) -> str:
    return f"""
You are an expert AXA Health customer contact analyst.

Analyse ONE customer contact episode (this may combine messages
from MOL and/or Telephony that happened close together). You are
given ONLY this episode's own conversation. You have NOT been
given any other episode - do not assume or invent information
about what happened before or after this episode.

TRANSCRIPT FORMAT
Each line in the conversation below may be prefixed with a
structural tag in square brackets, e.g.:
  [MOL] Customer_Query: <actual customer text>
  [MOL] Agent_FollowUp: [NO_EXPLICIT_CUSTOMER_MESSAGE]
  [MOL] Customer_Initiation: <timestamp> <agent name>: <agent text>

These tags, timestamps and agent names are STRUCTURAL METADATA,
not conversational content:
- "Customer_Query" means this agent message is a direct response
  to a customer message.
- "Agent_FollowUp" means the agent sent this message without an
  intervening customer reply (it may be a continuation, a
  clarification, or a chain of consecutive agent messages).
- "Customer_Initiation" means the conversation/episode opened via
  a website action with no typed customer message - the agent
  message that follows has no prior customer text to respond to.
- The literal placeholder text "[NO_EXPLICIT_CUSTOMER_MESSAGE]" is
  NOT something the customer said. Never quote it, paraphrase it,
  or treat it as customer language in any evidence field. Never
  let it influence sentiment_score - there is no customer
  language to score in that turn.
- Never use an agent's name or a timestamp as "evidence" for any
  field - evidence must always be a paraphrase of what someone
  actually said.

You MAY use the interaction-type pattern itself as evidence for
resolution_status and demand classification - e.g. a chain of
multiple consecutive Agent_FollowUp turns with no customer
response by the end of the episode is a meaningful signal that
the issue may be PartiallyResolved or Unclear rather than
Resolved, since there's no customer confirmation. State this kind
of reasoning explicitly in resolution_evidence when you use it.

Your task has four parts:
...
""".strip()