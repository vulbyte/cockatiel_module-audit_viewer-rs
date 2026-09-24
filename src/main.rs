use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent};
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Tabs},
    Frame, Terminal,
};
use tokio::sync::{broadcast, Mutex};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::{proto::container::Payload, proto::*, CockatielClient};
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsMessage,
>;
type WsReadHalf = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

// ── Engine handle (query + read plumbing) ─────────────────────────────

/// The engine-session identity (auth token + module + assigned instance).
/// Held in a shared Mutex so a reconnect can swap it in place while every other
/// task (read loop, poll task, TUI) keeps using the CURRENT session's
/// credentials — a stale token after a reconnect would be rejected by the
/// engine and the viewer would look dead.
#[derive(Clone, Default)]
struct SessionIdentity {
    auth_token: String,
    module_name: String,
    instance_uuid7: String,
}

#[derive(Clone)]
struct EngineHandle {
    identity: Arc<Mutex<SessionIdentity>>,
    write: Arc<Mutex<WsWriteHalf>>,
    results: Arc<Mutex<broadcast::Sender<DatabaseQueryResult>>>,
}

impl EngineHandle {
    fn new(
        auth_token: String,
        module_name: String,
        instance_uuid7: String,
        write: WsWriteHalf,
    ) -> Self {
        let (results, _) = broadcast::channel(256);
        Self {
            identity: Arc::new(Mutex::new(SessionIdentity {
                auth_token,
                module_name,
                instance_uuid7,
            })),
            write: Arc::new(Mutex::new(write)),
            results: Arc::new(Mutex::new(results)),
        }
    }

    async fn result_sender(&self) -> broadcast::Sender<DatabaseQueryResult> {
        self.results.lock().await.clone()
    }

    /// Swap in a fresh session (write half + identity + result channel) after a
    /// reconnect. Old db_query subscribers see Err(Closed) and fail fast
    /// instead of waiting on a dead connection.
    async fn swap_session(
        &self,
        auth_token: String,
        module_name: String,
        instance_uuid7: String,
        write: WsWriteHalf,
    ) {
        let (results, _) = broadcast::channel(256);
        *self.identity.lock().await = SessionIdentity {
            auth_token,
            module_name,
            instance_uuid7,
        };
        *self.write.lock().await = write;
        *self.results.lock().await = results;
    }

    pub async fn send_payload(&self, payload: Payload) -> Result<(), String> {
        let (auth_token, module_name, instance_uuid7) = {
            let id = self.identity.lock().await;
            (id.auth_token.clone(), id.module_name.clone(), id.instance_uuid7.clone())
        };
        let container = Container {
            version: 1,
            auth_token,
            module_name,
            module_instance_uuid7: instance_uuid7,
            payload: Some(payload),
        };
        let mut buf = Vec::new();
        container
            .encode(&mut buf)
            .map_err(|e| format!("encode error: {}", e))?;
        let mut write = self.write.lock().await;
        write
            .send(WsMessage::Binary(buf))
            .await
            .map_err(|e| format!("send error: {}", e))
    }

    /// Send a DatabaseQuery and wait for its matching DatabaseQueryResult.
    /// Fails fast: both the send and the wait are capped at 3s, so a dead
    /// socket or a slow query can never stall a refresh.
    async fn db_query(&self, query_id: &str, sql: &str) -> Result<DatabaseQueryResult, String> {
        let mut rx = self.results.lock().await.subscribe();
        let payload = Payload::DatabaseQuery(DatabaseQuery {
            query_id: query_id.to_string(),
            sql: sql.to_string(),
            params: vec![],
        });
        match tokio::time::timeout(Duration::from_secs(3), self.send_payload(payload)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(format!("send timed out for '{}'", query_id)),
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!("timed out waiting for '{}'", query_id));
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(res)) => {
                    if res.query_id == query_id {
                        return Ok(res);
                    }
                }
                Ok(Err(_)) => return Err("query channel closed".into()),
                Err(_) => return Err(format!("timed out waiting for '{}'", query_id)),
            }
        }
    }
}

// ── Data model ────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Row {
    platform: String,
    user: String,
    content: String,
    status: String,
    error: String,
    age: String,
}

#[derive(Clone, Default)]
struct AuditData {
    messages: Vec<Row>,
    errors: Vec<Row>,
    audit: Vec<Row>,
    // Archival timeline rows (event_type = 5): refreshed by the poll loop.
    logs: Vec<String>,
    // Live engine/module log feed: written by the read loop, NEVER touched by
    // refresh(). Capped so an idle viewer doesn't grow it forever.
    live_logs: Vec<String>,
    modules: Vec<serde_json::Value>,
    connected: bool,
    last_refresh: Option<String>,
    reconnect_msg: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Messages,
    Errors,
    Audit,
    Logs,
    Modules,
}

impl Tab {
    fn all() -> [Tab; 5] {
        [Tab::Messages, Tab::Errors, Tab::Audit, Tab::Logs, Tab::Modules]
    }
    fn label(&self) -> &'static str {
        match self {
            Tab::Messages => "Messages",
            Tab::Errors => "Errors",
            Tab::Audit => "Audit",
            Tab::Logs => "Logs",
            Tab::Modules => "Modules",
        }
    }
    fn idx(&self) -> usize {
        Tab::all().iter().position(|t| t == self).unwrap_or(0)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

fn cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn rel_time(ms: Option<i64>) -> String {
    let Some(ms) = ms else { return String::new() };
    let diff = ((now_ms() - ms) / 1000).max(0);
    if diff < 60 {
        format!("{}s", diff)
    } else if diff < 3600 {
        format!("{}m", diff / 60)
    } else {
        format!("{}h", diff / 3600)
    }
}

fn row_from_json(v: &serde_json::Value) -> Row {
    Row {
        platform: cell(v.get("platform").unwrap_or(&serde_json::Value::Null)),
        user: cell(v.get("user_uuid7").unwrap_or(&serde_json::Value::Null)),
        content: cell(v.get("raw_message").unwrap_or(&serde_json::Value::Null)),
        status: cell(v.get("pipeline_status").unwrap_or(&serde_json::Value::Null)),
        error: cell(v.get("error_message").unwrap_or(&serde_json::Value::Null)),
        age: rel_time(v.get("persisted_at").and_then(|x| x.as_i64())),
    }
}

const LIVE_LOG_CAP: usize = 500;

/// Append a live engine/module log line to the capped live buffer. This buffer
/// is owned solely by the read loop; refresh() never writes to it.
async fn push_live_log(data: &Arc<Mutex<AuditData>>, line: String) {
    let mut d = data.lock().await;
    d.live_logs.push(line);
    if d.live_logs.len() > LIVE_LOG_CAP {
        let over = d.live_logs.len() - LIVE_LOG_CAP;
        d.live_logs.drain(0..over);
    }
}

/// Dispatch a single engine payload: forward query results to the broadcast
/// channel, capture live engine/module logs, answer liveness probes.
async fn handle_payload(
    engine: &EngineHandle,
    results_tx: &broadcast::Sender<DatabaseQueryResult>,
    data: &Arc<Mutex<AuditData>>,
    payload: Payload,
) {
    match payload {
        // Answer the engine's liveness probe with our current auth token.
        Payload::AuthVerify(_) => {
            let cur_auth = engine.identity.lock().await.auth_token.clone();
            let _ = engine
                .send_payload(Payload::AuthVerify(AuthVerify { cur_auth }))
                .await;
        }
        Payload::DatabaseQueryResult(qr) => {
            let _ = results_tx.send(qr);
        }
        Payload::Log(log) => push_live_log(data, format!("[engine] {}", log.log)).await,
        Payload::Err(err) => push_live_log(data, format!("[error] {}", err.log)).await,
        Payload::ModuleControlResult(result) => {
            push_live_log(data, format!("[module] {}", result.message)).await
        }
        _ => {}
    }
}

/// Read from the engine until the socket drops (returns on Close / error /
/// stream end). Forwards query results and live logs into shared state.
async fn run_read_loop(mut read: WsReadHalf, engine: EngineHandle, data: Arc<Mutex<AuditData>>) {
    let results_tx = engine.result_sender().await;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(WsMessage::Binary(bin)) => {
                if let Ok(container) = Container::decode(bin.as_ref()) {
                    if let Some(payload) = container.payload {
                        handle_payload(&engine, &results_tx, &data, payload).await;
                    }
                }
            }
            // The engine dropped the socket — return so the supervisor can
            // mark the viewer disconnected and reconnect with backoff.
            Ok(WsMessage::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
}

// ── main ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::WARN).with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cockatiel = CockatielClient::connect("audit-viewer.json").await?;
    let (write, read) = cockatiel.stream.split();
    let engine = EngineHandle::new(
        cockatiel.auth_token.clone(),
        cockatiel.config.module_name.clone(),
        cockatiel.instance_uuid7.clone(),
        write,
    );
    let data: Arc<Mutex<AuditData>> = Arc::new(Mutex::new(AuditData {
        connected: true,
        ..Default::default()
    }));

    // Connection supervisor: owns the read loop and the reconnect lifecycle.
    // When the socket drops it marks the viewer disconnected (the poll loop
    // stops querying) and reconnects with exponential backoff (1s, 2s, 4s ...
    // cap 30s), swapping in a fresh session so every task uses the CURRENT
    // connection. This replaces the old behavior where the read task silently
    // exited on socket close and the poll task drew stale data forever.
    {
        let engine = engine.clone();
        let data = Arc::clone(&data);
        tokio::spawn(async move {
            let mut read = read;
            'session: loop {
                run_read_loop(read, engine.clone(), Arc::clone(&data)).await;

                // The engine connection dropped.
                {
                    let mut d = data.lock().await;
                    d.connected = false;
                    d.reconnect_msg = Some("disconnected — reconnecting...".to_string());
                }

                // Reconnect with exponential backoff (1s, 2s, 4s ... cap 30s).
                let mut backoff = 1u64;
                loop {
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    match CockatielClient::connect("audit-viewer.json").await {
                        Ok(conn) => {
                            let (w, r) = conn.stream.split();
                            engine
                                .swap_session(
                                    conn.auth_token.clone(),
                                    conn.config.module_name.clone(),
                                    conn.instance_uuid7.clone(),
                                    w,
                                )
                                .await;
                            read = r;
                            {
                                let mut d = data.lock().await;
                                d.connected = true;
                                d.reconnect_msg = None;
                            }
                            continue 'session;
                        }
                        Err(e) => {
                            backoff = (backoff * 2).min(30);
                            let mut d = data.lock().await;
                            d.reconnect_msg = Some(format!(
                                "reconnecting in {}s ({} failed)",
                                backoff, e
                            ));
                        }
                    }
                }
            }
        });
    }

    // Poll task: refresh the timeline + module list every few seconds, but only
    // while connected — a dead socket must not be hammered with db_query.
    {
        let data = Arc::clone(&data);
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                if data.lock().await.connected {
                    refresh(&engine, &data).await;
                }
            }
        });
    }

    // Enter the TUI.
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    crossterm::terminal::enable_raw_mode()?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_tui(&mut terminal, engine, data).await;
    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;
    if let Err(e) = result {
        eprintln!("Error: {}", e);
    }
    Ok(())
}

/// Run one timeline query and parse its rows. Returns None on any failure
/// (connection error, query failure, malformed blob) so a failed query simply
/// leaves the previous tab content in place.
async fn query_rows(engine: &EngineHandle, query_id: &str, sql: &str) -> Option<Vec<Row>> {
    let qr = engine.db_query(query_id, sql).await.ok()?;
    if !qr.success {
        return None;
    }
    let v = serde_json::from_slice::<serde_json::Value>(&qr.result_blob).ok()?;
    Some(
        v.as_array()
            .map(|a| a.iter().map(row_from_json).collect())
            .unwrap_or_default(),
    )
}

/// Archival timeline rows (module connect/disconnect, module logs, sends).
async fn query_archive_logs(engine: &EngineHandle) -> Option<Vec<String>> {
    let qr = engine
        .db_query(
            "audit_logs",
            "SELECT raw_message, flags, persisted_at FROM timeline_events WHERE event_type = 5 ORDER BY persisted_at DESC LIMIT 40",
        )
        .await
        .ok()?;
    if !qr.success {
        return None;
    }
    let v = serde_json::from_slice::<serde_json::Value>(&qr.result_blob).ok()?;
    let arr = v.as_array()?;
    let mut lines: Vec<String> = Vec::new();
    for r in arr {
        let msg = cell(r.get("raw_message").unwrap_or(&serde_json::Value::Null));
        let flags = cell(r.get("flags").unwrap_or(&serde_json::Value::Null));
        let age = rel_time(r.get("persisted_at").and_then(|x| x.as_i64()));
        let kind = serde_json::from_str::<serde_json::Value>(&flags)
            .ok()
            .and_then(|f| f.get("kind").cloned())
            .map(|v| cell(&v))
            .unwrap_or_default();
        lines.push(format!(
            "[{} {}] {}",
            age,
            if kind.is_empty() { "archive" } else { &kind },
            msg
        ));
    }
    Some(lines)
}

/// Module list (virtual query).
async fn query_modules(engine: &EngineHandle) -> Option<Vec<serde_json::Value>> {
    let qr = engine.db_query("module_list", "SELECT 1").await.ok()?;
    if !qr.success {
        return None;
    }
    let v = serde_json::from_slice::<serde_json::Value>(&qr.result_blob).ok()?;
    Some(v.as_array().cloned().unwrap_or_default())
}

async fn refresh(engine: &EngineHandle, data: &Arc<Mutex<AuditData>>) {
    // Run all five queries CONCURRENTLY — each capped at ~3s inside db_query —
    // so a single slow or hung query can never stall the whole refresh (old
    // behavior: five sequential 10s deadlines = 50s worst case). The live log
    // buffer is NOT part of the refresh; only the archival views are updated.
    let (msgs, errs, held, logs, mods) = tokio::join!(
        query_rows(
            engine,
            "audit_msgs",
            "SELECT platform, raw_message, user_uuid7, pipeline_status, error_message, persisted_at FROM timeline_events WHERE event_type = 1 ORDER BY persisted_at DESC LIMIT 60",
        ),
        query_rows(
            engine,
            "audit_err",
            "SELECT platform, raw_message, pipeline_status, error_message, persisted_at FROM timeline_events WHERE (error_message IS NOT NULL AND error_message != '') OR pipeline_status = 'failed' ORDER BY persisted_at DESC LIMIT 40",
        ),
        query_rows(
            engine,
            "audit_held",
            "SELECT platform, raw_message, pipeline_status, persisted_at FROM timeline_events WHERE pipeline_status = 'audit' ORDER BY persisted_at DESC LIMIT 40",
        ),
        query_archive_logs(engine),
        query_modules(engine),
    );

    let mut d = data.lock().await;
    if let Some(rows) = msgs {
        d.messages = rows;
    }
    if let Some(rows) = errs {
        d.errors = rows;
    }
    if let Some(rows) = held {
        d.audit = rows;
    }
    if let Some(lines) = logs {
        d.logs = lines;
    }
    if let Some(rows) = mods {
        d.modules = rows;
        d.last_refresh = Some(rel_time(Some(now_ms())));
    }
}

// ── TUI ───────────────────────────────────────────────────────────────

async fn run_tui(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    engine: EngineHandle,
    data: Arc<Mutex<AuditData>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tab = Tab::Messages;
    let mut scroll = 0usize;

    loop {
        // Redraw on a tick so live logs + data refresh smoothly.
        if event::poll(Duration::from_millis(250))? {
            match event::read()? {
                Event::Key(KeyEvent { code, modifiers, .. }) => {
                    match code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('r') => {
                            // Skip the refresh while disconnected — db_query on
                            // a dead socket would fail fast, but there's nothing
                            // to query for until the supervisor reconnects.
                            if data.lock().await.connected {
                                refresh(&engine, &data).await;
                            }
                        }
                        KeyCode::Tab => {
                            let next = (tab.idx() + 1) % Tab::all().len();
                            tab = Tab::all()[next];
                            scroll = 0;
                        }
                        KeyCode::BackTab => {
                            let prev = (tab.idx() + Tab::all().len() - 1) % Tab::all().len();
                            tab = Tab::all()[prev];
                            scroll = 0;
                        }
                        KeyCode::Char('j') | KeyCode::Down => scroll = scroll.saturating_add(1),
                        KeyCode::Char('k') | KeyCode::Up => scroll = scroll.saturating_sub(1),
                        _ => {}
                    }
                    // Shift+Tab arrives as BackTab already; ignore modifiers.
                    let _ = modifiers;
                }
                Event::Resize(_, _) => {}
                _ => {}
            }
        }

        let d = data.lock().await;
        terminal.draw(|f| draw(f, tab, scroll, &d))?;
    }
    Ok(())
}

fn draw(f: &mut Frame, tab: Tab, scroll: usize, d: &AuditData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(1),  // header
            Constraint::Length(2),  // tabs
            Constraint::Min(1),     // body
            Constraint::Length(1),  // footer
        ])
        .split(f.size());

    let status = if d.connected {
        "connected".to_string()
    } else {
        d.reconnect_msg
            .clone()
            .unwrap_or_else(|| "disconnected".to_string())
    };
    let status_color = if d.connected { Color::Green } else { Color::Yellow };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " COCKATIEL AUDIT VIEWER ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" · {}", status),
            Style::default().fg(status_color),
        ),
        Span::styled(
            format!(
                " · {} msgs · {} err · {} held · refresh {}",
                d.messages.len(),
                d.errors.len(),
                d.audit.len(),
                d.last_refresh.as_deref().unwrap_or("-")
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    f.render_widget(header, chunks[0]);

    let titles: Vec<Line> = Tab::all()
        .iter()
        .map(|t| {
            if *t == tab {
                Line::from(Span::styled(
                    format!(" {} ", t.label()),
                    Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
                ))
            } else {
                Line::from(Span::styled(format!(" {} ", t.label()), Style::default().fg(Color::Gray)))
            }
        })
        .collect();
    f.render_widget(Tabs::new(titles).block(Block::default().borders(Borders::BOTTOM)), chunks[1]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(20), Constraint::Percentage(80)])
        .split(chunks[2]);

    // Left column: a short "what am I looking at" / module summary.
    let left_lines: Vec<ListItem> = d
        .modules
        .iter()
        .map(|m| {
            let name = cell(m.get("name").unwrap_or(&serde_json::Value::Null));
            let st = cell(m.get("status").unwrap_or(&serde_json::Value::Null));
            let pos = cell(m.get("position").unwrap_or(&serde_json::Value::Null));
            ListItem::new(Line::from(vec![
                Span::raw("  "),
                Span::styled(name, Style::default().fg(Color::White)),
                Span::styled(format!(" [{}]", pos), Style::default().fg(Color::DarkGray)),
                Span::styled(format!(" {}", st), Style::default().fg(color_for_status(&st))),
            ]))
        })
        .collect();
    let left_block = Block::default().title(" modules ").borders(Borders::ALL);
    f.render_widget(List::new(left_lines).block(left_block), body[0]);

    // Right column: the active tab's content.
    let right_block = Block::default().title(format!(" {} ", tab.label())).borders(Borders::ALL);
    let right_area = body[1];

    let items: Vec<ListItem> = match tab {
        Tab::Messages => d
            .messages
            .iter()
            .map(|r| {
                let color = color_for_status(&r.status);
                ListItem::new(vec![Line::from(vec![
                    Span::styled(format!("[{}]", r.platform), Style::default().fg(platform_color(&r.platform))),
                    Span::styled(format!(" {} ", r.user), Style::default().fg(Color::Yellow)),
                    Span::raw(" "),
                    Span::raw(truncate(&r.content, 90)),
                ]), Line::from(vec![
                    Span::styled(format!("   {}", r.age), Style::default().fg(Color::DarkGray)),
                    Span::styled(format!(" {}", r.status), Style::default().fg(color)),
                ])])
            })
            .collect(),
        Tab::Errors => d
            .errors
            .iter()
            .map(|r| {
                ListItem::new(vec![Line::from(vec![
                    Span::styled(format!("[{}]", r.platform), Style::default().fg(Color::Red)),
                    Span::raw(" "),
                    Span::raw(truncate(&r.content, 90)),
                ]), Line::from(vec![
                    Span::styled(format!("   {}", r.age), Style::default().fg(Color::DarkGray)),
                    Span::styled(format!(" {} ", r.status), Style::default().fg(Color::Red)),
                    Span::raw(truncate(&r.error, 60)),
                ])])
            })
            .collect(),
        Tab::Audit => d
            .audit
            .iter()
            .map(|r| {
                ListItem::new(vec![Line::from(vec![
                    Span::styled(format!("[{}]", r.platform), Style::default().fg(platform_color(&r.platform))),
                    Span::styled(format!(" {} ", r.user), Style::default().fg(Color::Yellow)),
                    Span::raw(" "),
                    Span::raw(truncate(&r.content, 90)),
                ]), Line::from(vec![
                    Span::styled(format!("   {}", r.age), Style::default().fg(Color::DarkGray)),
                    Span::styled(" held for review", Style::default().fg(Color::Magenta)),
                ])])
            })
            .collect(),
        // The Logs tab shows BOTH the live engine/module feed (read loop, never
        // clobbered by refresh) and the archival timeline rows (refresh).
        Tab::Logs => {
            let mut items: Vec<ListItem> = Vec::new();
            if !d.live_logs.is_empty() {
                items.push(ListItem::new(Line::from(vec![Span::styled(
                    "── live feed ──",
                    Style::default().fg(Color::DarkGray),
                )])));
                items.extend(
                    d.live_logs
                        .iter()
                        .map(|l| ListItem::new(Line::from(vec![Span::raw(truncate(l, 120))]))),
                );
            }
            if !d.logs.is_empty() {
                items.push(ListItem::new(Line::from(vec![Span::styled(
                    "── archive ──",
                    Style::default().fg(Color::DarkGray),
                )])));
                items.extend(
                    d.logs
                        .iter()
                        .map(|l| ListItem::new(Line::from(vec![Span::raw(truncate(l, 120))]))),
                );
            }
            items
        }
        Tab::Modules => d
            .modules
            .iter()
            .map(|m| {
                let name = cell(m.get("name").unwrap_or(&serde_json::Value::Null));
                let st = cell(m.get("status").unwrap_or(&serde_json::Value::Null));
                let pos = cell(m.get("position").unwrap_or(&serde_json::Value::Null));
                let desc = cell(m.get("description").unwrap_or(&serde_json::Value::Null));
                ListItem::new(vec![Line::from(vec![
                    Span::styled(format!("{:<22}", name), Style::default().fg(Color::White)),
                    Span::styled(format!("{:<12}", pos), Style::default().fg(Color::DarkGray)),
                    Span::styled(st.clone(), Style::default().fg(color_for_status(&st))),
                ]), Line::from(vec![
                    Span::styled(format!("   {}", truncate(&desc, 100)), Style::default().fg(Color::DarkGray)),
                ])])
            })
            .collect(),
    };

    // Render with scroll offset (start from `scroll`).
    let start = scroll.min(items.len().saturating_sub(1));
    let vis = if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  (nothing here yet)",
            Style::default().fg(Color::DarkGray),
        )))]
    } else {
        items.iter().skip(start).cloned().collect()
    };
    let list = List::new(vis).block(right_block.clone());
    f.render_widget(list, right_area);

    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" Tab/Shift+Tab: switch · j/k: scroll · r: refresh · q: quit ", Style::default().fg(Color::DarkGray)),
    ]));
    f.render_widget(footer, chunks[3]);
}

fn truncate(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('…');
    }
    out
}

fn color_for_status(status: &str) -> Color {
    match status {
        "complete" => Color::Green,
        "queued" => Color::Blue,
        "processing" => Color::Yellow,
        "failed" => Color::Red,
        "audit" => Color::Magenta,
        "connected" => Color::Green,
        "starting" => Color::Yellow,
        "disconnected" => Color::Yellow,
        "offline" => Color::DarkGray,
        "error" => Color::LightRed,
        "crashed" => Color::Red,
        _ => Color::Gray,
    }
}

fn platform_color(p: &str) -> Color {
    match p {
        "twitch" => Color::Magenta,
        "youtube" => Color::Red,
        "kick" => Color::Green,
        "discord" => Color::Blue,
        _ => Color::Cyan,
    }
}