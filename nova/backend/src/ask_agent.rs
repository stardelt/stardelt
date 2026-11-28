//! Ask's agent loop — slice 1 of stardelt/stardelt#2344 (stardelt/stardelt#2365).
//!
//! In agent mode Ask talks to the model over several turns, offering it the platform's
//! **read-only** MCP tools, plus the non-destructive write tools outside production. The
//! rules of this module:
//!
//! - **As the person.** Only a tool whose `annotations.readOnlyHint` is `true` is offered;
//!   outside a production stage a tool that is neither read-only nor destructive (a
//!   documentation, eval or notebook write) is offered too, never a destructive one
//!   (`pipeline_start`). A call naming anything else is answered to the model with
//!   [`NOT_AVAILABLE`] and is never run. The caller runs the tools with that person's own
//!   `Principal` and the read scope, plus the write scope when it offered the writes; the loop
//!   here only decides *which* call is allowed, never as whom it runs.
//! - **A write is visible.** A write call's step list line reads `wrote: <tool> <target>`, so
//!   the person sees what changed and where.
//! - **Bounded.** At most [`MAX_MODEL_CALLS`] model calls and [`MAX_TOOL_CALLS`] tool calls
//!   per question, and every tool result is cut to [`MAX_TOOL_RESULT_CHARS`] characters
//!   before it goes back to the model.
//! - **Rows follow the endpoint.** When the endpoint's `sendRows` is off, a `sql_read`
//!   result reaches the model as its columns and row count only ([`without_rows`]); the
//!   person still sees the rows in the step list.
//!
//! The loop is generic over the two calls it makes, so the tests drive it with closures and
//! need no network and no cluster.

use std::future::Future;

use serde::Serialize;
use serde_json::{Value, json};

use crate::model_chat::{Answer, ChatError};

/// How many model calls one question may make (D3).
pub(crate) const MAX_MODEL_CALLS: usize = 8;
/// How many tool calls one question may make (D3).
pub(crate) const MAX_TOOL_CALLS: usize = 6;
/// The longest tool result that goes back to the model (D3).
pub(crate) const MAX_TOOL_RESULT_CHARS: usize = 8_000;
/// How many rows of a `sql_read` the answer keeps for the panel (D5).
const STEP_ROWS: usize = 50;
/// The last message the model gets when the tool cap is reached (D3).
pub(crate) const ANSWER_NOW: &str = "Answer now with what you have, in words.";
/// What the model hears when it calls a tool Ask does not offer (D2).
pub(crate) const NOT_AVAILABLE: &str = "this tool is not available to Ask";

/// One tool call an agent answer made, for the panel (D5).
#[derive(Clone, Debug, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskStep {
    /// The MCP tool the model called.
    pub tool: String,
    /// The arguments it passed, as sent.
    #[schema(value_type = Object)]
    pub arguments: Value,
    /// One line for the step list, for example `sql_read: 12 rows`.
    pub summary: String,
    /// At most 50 rows of a `sql_read`, for the panel only; `null` for every other tool.
    #[schema(value_type = Option<Vec<Vec<Value>>>)]
    pub rows: Option<Value>,
}

/// What the loop ended with.
pub(crate) struct AgentOutcome {
    /// The model's answer in words; `None` when it never answered without a tool call.
    pub text: Option<String>,
    /// The tool calls it made, in order.
    pub steps: Vec<AskStep>,
    /// The last `sql_read` statement it ran, for "Open in SQL editor".
    pub last_sql: Option<String>,
}

/// The MCP tool list as an OpenAI `tools` array: the read-only tools, plus — with `writes`
/// — the tools that change state without destroying any (`readOnlyHint: false` and not
/// `destructiveHint: true`). A destructive tool is never offered (D2).
pub(crate) fn openai_tools(mcp_tools: &Value, writes: bool) -> Value {
    let tools: Vec<Value> = mcp_tools
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .filter(|tool| {
            if tool.pointer("/annotations/readOnlyHint") == Some(&Value::Bool(true)) {
                return true;
            }
            writes
                && tool.pointer("/annotations/readOnlyHint") == Some(&Value::Bool(false))
                && tool.pointer("/annotations/destructiveHint") != Some(&Value::Bool(true))
        })
        .filter_map(|tool| {
            let name = tool.get("name")?.as_str()?;
            Some(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool.get("description").cloned().unwrap_or(Value::Null),
                    "parameters": tool
                        .get("inputSchema")
                        .cloned()
                        .unwrap_or_else(|| json!({"type": "object"})),
                },
            }))
        })
        .collect();
    Value::Array(tools)
}

/// Whether `name` is one of the tools the model was offered (D2). A write tool is never in
/// that list, so it is never allowed.
pub(crate) fn allowed(name: &str, offered: &Value) -> bool {
    offered
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .any(|tool| tool.pointer("/function/name").and_then(Value::as_str) == Some(name))
}

/// A tool result cut to `max_chars` characters of its JSON (D3). Under the limit it is
/// returned unchanged; past it the model gets the prefix and `truncated: true`.
pub(crate) fn cut(result: Value, max_chars: usize) -> Value {
    let text = serde_json::to_string(&result).unwrap_or_else(|_| "null".into());
    if text.chars().count() <= max_chars {
        return result;
    }
    let kept: String = text.chars().take(max_chars).collect();
    json!({ "truncated": true, "text": kept })
}

/// A `sql_read` result without its rows, for an endpoint whose `sendRows` is off (D4).
pub(crate) fn without_rows(result: &Value) -> Value {
    json!({
        "columns": result.get("columns").cloned().unwrap_or_else(|| json!([])),
        "rowCount": result.get("rowCount").cloned().unwrap_or_else(|| json!(0)),
        "note": "rows are not sent to this model (sendRows is off)",
    })
}

/// One line for the step list. `sql_read` reports its row count; a write says `wrote: <tool>
/// <target>` so the person sees what changed; a read tool names the table or the argument it
/// looked at.
fn summary_of(name: &str, arguments: &Value, result: &Value) -> String {
    if name == "sql_read" {
        let rows = result
            .get("rowCount")
            .and_then(Value::as_u64)
            .or_else(|| {
                result
                    .get("rows")
                    .and_then(Value::as_array)
                    .map(|rows| rows.len() as u64)
            })
            .unwrap_or(0);
        return format!("sql_read: {rows} rows");
    }
    let part = |key: &str| arguments.get(key).and_then(Value::as_str);
    let target = if let (Some(namespace), Some(table)) = (part("namespace"), part("table")) {
        Some(format!("{namespace}.{table}"))
    } else if let (Some(group), Some(notebook)) = (part("group"), part("name")) {
        Some(format!("{group}/{notebook}"))
    } else {
        ["catalog", "query", "pipeline", "id"]
            .iter()
            .find_map(|key| part(key))
            .map(str::to_string)
    };
    if crate::mcp::required_scope(name) == crate::mcp::WRITE_SCOPE {
        return match target {
            Some(target) => format!("wrote: {name} {target}"),
            None => format!("wrote: {name}"),
        };
    }
    match target {
        Some(target) => format!("{name}: {target}"),
        None => name.to_string(),
    }
}

/// The rows of a `sql_read` the panel keeps, at most [`STEP_ROWS`].
fn rows_of(result: &Value) -> Option<Value> {
    let rows = result.get("rows")?.as_array()?;
    Some(Value::Array(rows.iter().take(STEP_ROWS).cloned().collect()))
}

/// The agent loop: ask the model, run the tools it asks for, until it answers in words or a
/// cap is reached (D2, D3).
///
/// `model(messages, with_tools)` is one chat call; `tool(name, arguments)` runs one tool and
/// answers its JSON result. Both are closures so the tests drive the loop without a network.
pub(crate) async fn run<M, T, MF, TF>(
    mut model: M,
    mut tool: T,
    messages: Value,
    offered: &Value,
    send_rows: bool,
) -> Result<AgentOutcome, ChatError>
where
    M: FnMut(Value, bool) -> MF,
    MF: Future<Output = Result<Answer, ChatError>>,
    T: FnMut(String, Value) -> TF,
    TF: Future<Output = Value>,
{
    let mut messages: Vec<Value> = messages.as_array().cloned().unwrap_or_default();
    let mut steps: Vec<AskStep> = Vec::new();
    let mut last_sql: Option<String> = None;
    let mut tool_calls = 0usize;
    let mut model_calls = 0usize;
    let mut asked_final = false;
    let mut text: Option<String> = None;

    while model_calls < MAX_MODEL_CALLS {
        let with_tools = tool_calls < MAX_TOOL_CALLS;
        let answer = model(Value::Array(messages.clone()), with_tools).await?;
        model_calls += 1;
        messages.push(answer.message.clone());
        let calls: Vec<Value> = answer
            .message
            .get("tool_calls")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // A call naming a write tool or an unknown one is answered, never run — and with the
        // tools withdrawn there is nothing left to run at all.
        if calls.is_empty() || !with_tools {
            let words = answer.content.trim();
            text = (!words.is_empty()).then(|| words.to_string());
            break;
        }
        for call in calls {
            if tool_calls >= MAX_TOOL_CALLS {
                break;
            }
            let name = call
                .pointer("/function/name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let arguments = call
                .pointer("/function/arguments")
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .unwrap_or(Value::Null);
            let id = call.get("id").cloned().unwrap_or(Value::Null);
            let result = if name.is_empty() || !allowed(&name, offered) {
                json!({ "error": NOT_AVAILABLE })
            } else {
                tool_calls += 1;
                let raw = tool(name.clone(), arguments.clone()).await;
                if name == "sql_read" {
                    last_sql = arguments
                        .get("sql")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                steps.push(AskStep {
                    summary: summary_of(&name, &arguments, &raw),
                    rows: (name == "sql_read").then(|| rows_of(&raw)).flatten(),
                    tool: name.clone(),
                    arguments: arguments.clone(),
                });
                if name == "sql_read" && !send_rows {
                    without_rows(&raw)
                } else {
                    raw
                }
            };
            messages.push(json!({
                "role": "tool",
                "tool_call_id": id,
                "content": cut(result, MAX_TOOL_RESULT_CHARS).to_string(),
            }));
        }
        if tool_calls >= MAX_TOOL_CALLS && !asked_final {
            asked_final = true;
            messages.push(json!({ "role": "user", "content": ANSWER_NOW }));
        }
    }
    Ok(AgentOutcome {
        text,
        steps,
        last_sql,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The MCP tools the endpoint offers: two read-only ones and the one write tool.
    fn mcp_tools() -> Value {
        json!([
            {
                "name": "catalog_search",
                "description": "Find tables by name",
                "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
                "annotations": {"readOnlyHint": true},
            },
            {
                "name": "sql_read",
                "description": "Run one read-only statement",
                "inputSchema": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]},
                "annotations": {"readOnlyHint": true},
            },
            {
                "name": "table_documentation",
                "description": "What a table means",
                "inputSchema": {"type": "object", "properties": {"namespace": {"type": "string"}, "table": {"type": "string"}}, "required": ["namespace", "table"]},
                "annotations": {"readOnlyHint": true},
            },
            {
                "name": "pipeline_start",
                "description": "Start one run",
                "inputSchema": {"type": "object", "properties": {"pipeline": {"type": "string"}, "reason": {"type": "string"}}, "required": ["pipeline", "reason"]},
                "annotations": {"readOnlyHint": false, "destructiveHint": true},
            },
        ])
    }

    /// One assistant message that asks for a tool.
    fn tool_call(name: &str, arguments: Value) -> Answer {
        Answer {
            content: String::new(),
            message: json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": name, "arguments": arguments.to_string()},
                }],
            }),
            usage: None,
            finish_reason: None,
            latency_ms: 1,
        }
    }

    /// One assistant message that answers in words.
    fn words(text: &str) -> Answer {
        Answer {
            content: text.to_string(),
            message: json!({"role": "assistant", "content": text}),
            usage: None,
            finish_reason: None,
            latency_ms: 1,
        }
    }

    /// The loop's outcome, or the failure's sentence (the error carries no `Debug`).
    fn outcome_of(result: Result<AgentOutcome, ChatError>) -> AgentOutcome {
        match result {
            Ok(outcome) => outcome,
            Err(ChatError::Refused(message) | ChatError::Transport(message)) => {
                panic!("the loop failed: {message}")
            }
        }
    }

    /// A `sql_read` result as `mcp::call_tool` answers it.
    fn sql_result() -> Value {
        json!({
            "columns": [{"name": "id", "type": "bigint"}, {"name": "name", "type": "varchar"}],
            "rows": [[1, "a"], [2, "b"]],
            "rowCount": 2,
            "queryId": "q-1",
            "stats": null,
            "truncated": false,
        })
    }

    #[test]
    fn openai_tools_offers_only_read_only_tools() {
        let offered = openai_tools(&mcp_tools(), false);
        let names: Vec<&str> = offered
            .as_array()
            .expect("a list")
            .iter()
            .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
            .collect();
        assert_eq!(
            names,
            vec!["catalog_search", "sql_read", "table_documentation"],
            "the write tool must not be offered"
        );
        // The shape is OpenAI's: the schema moves under `function.parameters`.
        let search = &offered[0];
        assert_eq!(search["type"], json!("function"));
        assert_eq!(
            search["function"]["description"],
            json!("Find tables by name")
        );
        assert_eq!(
            search["function"]["parameters"]["properties"]["query"]["type"],
            json!("string")
        );
        assert!(!allowed("pipeline_start", &offered));
        assert!(allowed("sql_read", &offered));
    }

    #[test]
    fn writes_are_offered_only_outside_production() {
        let tools = crate::mcp::tools();
        // A development stage: the non-destructive writes are offered, the destructive one is not.
        let offered = openai_tools(&tools, true);
        assert!(allowed("notebook_save", &offered));
        assert!(allowed("table_documentation_update", &offered));
        assert!(!allowed("pipeline_start", &offered));
        // Production (or a client that asked for reads): neither write is offered.
        let offered = openai_tools(&tools, false);
        assert!(!allowed("notebook_save", &offered));
        assert!(!allowed("table_documentation_update", &offered));
        assert!(!allowed("pipeline_start", &offered));
        // Every read tool is offered either way.
        assert!(allowed("notebooks_list", &offered));
        assert!(allowed("notebook_read", &offered));
    }

    #[test]
    fn a_destructive_tool_is_never_offered() {
        let offered = openai_tools(&crate::mcp::tools(), true);
        let names: Vec<&str> = offered
            .as_array()
            .expect("a list")
            .iter()
            .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
            .collect();
        assert!(
            !names.contains(&"pipeline_start"),
            "a destructive tool reached the model: {names:?}"
        );
        assert!(names.contains(&"notebook_save"), "{names:?}");
        // The fixture's destructive tool is refused with `writes` on, too.
        let offered = openai_tools(&mcp_tools(), true);
        assert!(!allowed("pipeline_start", &offered));
    }

    #[test]
    fn a_write_step_says_wrote_and_where() {
        assert_eq!(
            summary_of(
                "notebook_save",
                &json!({"group": "demo", "name": "revenue.ipynb"}),
                &json!({"objectVersion": "v1"})
            ),
            "wrote: notebook_save demo/revenue.ipynb"
        );
        assert_eq!(
            summary_of(
                "table_documentation_update",
                &json!({"namespace": "gold", "table": "shop", "reason": "clarify"}),
                &json!({"ok": true})
            ),
            "wrote: table_documentation_update gold.shop"
        );
        // A read tool keeps its colon form.
        assert_eq!(
            summary_of("catalog_search", &json!({"query": "trips"}), &json!({})),
            "catalog_search: trips"
        );
    }

    #[tokio::test]
    async fn the_loop_runs_a_tool_and_answers_in_words() {
        let offered = openai_tools(&mcp_tools(), false);
        let mut answers = vec![
            tool_call("catalog_search", json!({"query": "trips"})),
            words("Two tables hold trips."),
        ]
        .into_iter();
        let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let model = {
            let seen = Arc::clone(&seen);
            move |messages: Value, _with_tools: bool| {
                seen.lock().expect("the log").push(messages);
                let answer = answers.next().expect("two prepared answers");
                async move { Ok(answer) }
            }
        };
        let ran: Arc<Mutex<Vec<(String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let tool = {
            let ran = Arc::clone(&ran);
            move |name: String, arguments: Value| {
                ran.lock().expect("the log").push((name, arguments));
                async {
                    json!({"matches": [{"catalog": "warehouse", "namespace": "bronze", "table": "trips"}]})
                }
            }
        };
        let outcome = run(
            model,
            tool,
            json!([{"role": "user", "content": "what is in trips?"}]),
            &offered,
            true,
        )
        .await;
        let outcome = outcome_of(outcome);

        assert_eq!(outcome.steps.len(), 1);
        assert_eq!(outcome.steps[0].tool, "catalog_search");
        assert_eq!(outcome.steps[0].summary, "catalog_search: trips");
        assert_eq!(outcome.steps[0].rows, None);
        assert_eq!(outcome.text.as_deref(), Some("Two tables hold trips."));
        assert_eq!(outcome.last_sql, None);
        assert_eq!(ran.lock().expect("the log").len(), 1);
        // The model's second call carries the tool's answer as a `tool` message.
        let second = seen.lock().expect("the log")[1].clone();
        let last = second
            .as_array()
            .expect("a list")
            .last()
            .expect("a message");
        assert_eq!(last["role"], json!("tool"));
        assert_eq!(last["tool_call_id"], json!("call-1"));
        assert!(
            last["content"]
                .as_str()
                .expect("a string")
                .contains("trips")
        );
    }

    #[tokio::test]
    async fn a_write_tool_is_never_run() {
        let offered = openai_tools(&mcp_tools(), false);
        let mut answers = vec![
            tool_call(
                "pipeline_start",
                json!({"pipeline": "shop/orders", "reason": "I want to"}),
            ),
            words("I cannot start pipelines."),
        ]
        .into_iter();
        let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let model = {
            let seen = Arc::clone(&seen);
            move |messages: Value, _with_tools: bool| {
                seen.lock().expect("the log").push(messages);
                let answer = answers.next().expect("two prepared answers");
                async move { Ok(answer) }
            }
        };
        let ran: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let tool = {
            let ran = Arc::clone(&ran);
            move |_name: String, _arguments: Value| {
                *ran.lock().expect("the log") += 1;
                async { json!({"started": true}) }
            }
        };
        let outcome = run(
            model,
            tool,
            json!([{"role": "user", "content": "start the pipeline"}]),
            &offered,
            true,
        )
        .await;
        let outcome = outcome_of(outcome);

        assert_eq!(*ran.lock().expect("the log"), 0, "a write tool was run");
        assert!(outcome.steps.is_empty());
        let second = seen.lock().expect("the log")[1].clone();
        let last = second
            .as_array()
            .expect("a list")
            .last()
            .expect("a message");
        assert_eq!(last["role"], json!("tool"));
        assert!(
            last["content"]
                .as_str()
                .expect("a string")
                .contains(NOT_AVAILABLE),
            "{last}"
        );
    }

    #[tokio::test]
    async fn the_loop_stops_at_six_tool_calls_and_asks_for_an_answer() {
        let offered = openai_tools(&mcp_tools(), false);
        let seen: Arc<Mutex<Vec<(Value, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let model = {
            let seen = Arc::clone(&seen);
            move |messages: Value, with_tools: bool| {
                seen.lock().expect("the log").push((messages, with_tools));
                let answer = if with_tools {
                    tool_call("catalog_search", json!({"query": "more"}))
                } else {
                    words("That is all I found.")
                };
                async move { Ok(answer) }
            }
        };
        let ran: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let tool = {
            let ran = Arc::clone(&ran);
            move |_name: String, _arguments: Value| {
                *ran.lock().expect("the log") += 1;
                async { json!({"matches": []}) }
            }
        };
        let outcome = run(
            model,
            tool,
            json!([{"role": "user", "content": "find everything"}]),
            &offered,
            true,
        )
        .await;
        let outcome = outcome_of(outcome);

        assert_eq!(*ran.lock().expect("the log"), MAX_TOOL_CALLS);
        assert_eq!(outcome.steps.len(), MAX_TOOL_CALLS);
        assert_eq!(outcome.text.as_deref(), Some("That is all I found."));
        let log = seen.lock().expect("the log");
        let (last_messages, with_tools) = log.last().expect("a last call").clone();
        assert!(!with_tools, "the last call must withdraw the tools");
        let last = last_messages
            .as_array()
            .expect("a list")
            .last()
            .expect("a message")
            .clone();
        assert_eq!(last["role"], json!("user"));
        assert_eq!(last["content"], json!(ANSWER_NOW));
        // Six tool calls, then the call that answers: seven model calls, within the cap.
        assert_eq!(log.len(), MAX_TOOL_CALLS + 1);
        assert!(log.len() <= MAX_MODEL_CALLS);
    }

    #[tokio::test]
    async fn rows_do_not_reach_the_model_without_send_rows() {
        let offered = openai_tools(&mcp_tools(), false);
        let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let tool = || {
            let _ = ();
            move |_name: String, _arguments: Value| async { sql_result() }
        };

        // sendRows off: the model gets the columns and the count, the step keeps the rows.
        let mut answers = vec![
            tool_call("sql_read", json!({"sql": "SELECT id, name FROM bronze.t"})),
            words("Two rows."),
        ]
        .into_iter();
        let model = {
            let seen = Arc::clone(&seen);
            move |messages: Value, _with_tools: bool| {
                seen.lock().expect("the log").push(messages);
                let answer = answers.next().expect("two prepared answers");
                async move { Ok(answer) }
            }
        };
        let outcome = run(
            model,
            tool(),
            json!([{"role": "user", "content": "what is in t?"}]),
            &offered,
            false,
        )
        .await;
        let outcome = outcome_of(outcome);

        let second = seen.lock().expect("the log")[1].clone();
        let last = second
            .as_array()
            .expect("a list")
            .last()
            .expect("a message");
        let sent = last["content"].as_str().expect("a string");
        assert!(sent.contains("\"rowCount\":2"), "{sent}");
        assert!(sent.contains("sendRows is off"), "{sent}");
        assert!(!sent.contains("[1,"), "a row reached the model: {sent}");
        assert!(!sent.contains("\"rows\""), "{sent}");
        // The person still sees the rows and the statement.
        assert_eq!(outcome.steps[0].summary, "sql_read: 2 rows");
        assert_eq!(outcome.steps[0].rows, Some(json!([[1, "a"], [2, "b"]])));
        assert_eq!(
            outcome.last_sql.as_deref(),
            Some("SELECT id, name FROM bronze.t")
        );

        // sendRows on: the same result, rows and all, is what the model receives.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut answers = vec![
            tool_call("sql_read", json!({"sql": "SELECT id, name FROM bronze.t"})),
            words("Two rows."),
        ]
        .into_iter();
        let model = {
            let seen = Arc::clone(&seen);
            move |messages: Value, _with_tools: bool| {
                seen.lock().expect("the log").push(messages);
                let answer = answers.next().expect("two prepared answers");
                async move { Ok(answer) }
            }
        };
        outcome_of(
            run(
                model,
                tool(),
                json!([{"role": "user", "content": "what is in t?"}]),
                &offered,
                true,
            )
            .await,
        );
        let second = seen.lock().expect("the log")[1].clone();
        let last = second
            .as_array()
            .expect("a list")
            .last()
            .expect("a message");
        assert!(
            last["content"]
                .as_str()
                .expect("a string")
                .contains("[[1,\"a\"],[2,\"b\"]]"),
            "{last}"
        );
    }

    #[test]
    fn a_long_tool_result_is_cut_and_says_so() {
        let big = json!({"text": "x".repeat(100)});
        let cut_down = cut(big.clone(), 20);
        assert_eq!(cut_down["truncated"], json!(true));
        let kept = cut_down["text"].as_str().expect("a string");
        assert_eq!(kept.chars().count(), 20);
        // Under the limit nothing changes.
        assert_eq!(cut(json!({"ok": true}), 20), json!({"ok": true}));
    }
}
