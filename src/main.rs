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

// ── Engine handle (query + read plumbing) ─────────────────────────────

#[derive(Clone)]
struct EngineHandle {
    auth_token: String,
    module_name: String,
    instance_uuid7: String,
    write: Arc<Mutex<WsWriteHalf>>,
    results: broadcast::Sender<DatabaseQueryResult>,
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
            auth_token,
            module_name,
            instance_uuid7,
            write: Arc::new(Mutex::new(write)),
            results,
        }
    }

    fn result_sender(&self) -> broadcast::Sender<DatabaseQueryResult> {
        self.results.clone()
    }

    pub async fn send_payload(&self, payload: Payload) -> Result<(), String> {
        let container = Container {
            version: 1,
            auth_token: self.auth_token.clone(),
            module_name: self.module_name.clone(),
            module_instance_uuid7: self.instance_uuid7.clone(),
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
    async fn db_query(&self, query_id: &str, sql: &str) -> Result<DatabaseQueryResult, String> {
        let mut rx = self.results.subscribe();
        self.send_payload(Payload::DatabaseQuery(DatabaseQuery {
            query_id: query_id.to_string(),
            sql: sql.to_string(),
            params: vec![],
        }))
        .await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
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
    logs: Vec<String>,
    modules: Vec<serde_json::Value>,
    connected: bool,
    last_refresh: Option<String>,
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
    let data: Arc<Mutex<AuditData>> = Arc::new(Mutex::new(AuditData::default()));

    // Read task: forward query results, capture live engine/module logs.
    {
        let results_tx = engine.result_sender();
        let data = Arc::clone(&data);
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                let Ok(WsMessage::Binary(bin)) = msg else { continue };
                let Ok(container) = Container::decode(bin.as_ref()) else { continue };
                let Some(payload) = container.payload else { continue };
                match payload {
                    // Answer the engine's liveness probe with our auth token.
                    Payload::AuthVerify(_) => {
                        let _ = engine
                            .send_payload(Payload::AuthVerify(AuthVerify {
                                cur_auth: engine.auth_token.clone(),
                            }))
                            .await;
                    }
                    Payload::DatabaseQueryResult(qr) => {
                        let _ = results_tx.send(qr);
                    }
                    Payload::Log(log) => {
                        let mut d = data.lock().await;
                        d.logs.push(format!("[engine] {}", log.log));
                        if d.logs.len() > 400 {
                            d.logs.remove(0);
                        }
                    }
                    Payload::Err(err) => {
                        let mut d = data.lock().await;
                        d.logs.push(format!("[error] {}", err.log));
                        if d.logs.len() > 400 {
                            d.logs.remove(0);
                        }
                    }
                    Payload::ModuleControlResult(result) => {
                        let mut d = data.lock().await;
                        d.logs.push(format!("[module] {}", result.message));
                        if d.logs.len() > 400 {
                            d.logs.remove(0);
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // Poll task: refresh the timeline + module list every few seconds.
    {
        let data = Arc::clone(&data);
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                refresh(&engine, &data).await;
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

async fn refresh(engine: &EngineHandle, data: &Arc<Mutex<AuditData>>) {
    // Recent chat messages with pipeline status.
    if let Ok(qr) = engine
        .db_query(
            "audit_msgs",
            "SELECT platform, raw_message, user_uuid7, pipeline_status, error_message, persisted_at FROM timeline_events WHERE event_type = 1 ORDER BY persisted_at DESC LIMIT 60",
        )
        .await
    {
        if qr.success {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&qr.result_blob) {
                let rows = v
                    .as_array()
                    .map(|a| a.iter().map(row_from_json).collect())
                    .unwrap_or_default();
                let mut d = data.lock().await;
                d.messages = rows;
            }
        }
    }

    // Error / failed rows.
    if let Ok(qr) = engine
        .db_query(
            "audit_err",
            "SELECT platform, raw_message, pipeline_status, error_message, persisted_at FROM timeline_events WHERE (error_message IS NOT NULL AND error_message != '') OR pipeline_status = 'failed' ORDER BY persisted_at DESC LIMIT 40",
        )
        .await
    {
        if qr.success {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&qr.result_blob) {
                let rows = v
                    .as_array()
                    .map(|a| a.iter().map(row_from_json).collect())
                    .unwrap_or_default();
                let mut d = data.lock().await;
                d.errors = rows;
            }
        }
    }

    // Held-for-audit messages.
    if let Ok(qr) = engine
        .db_query(
            "audit_held",
            "SELECT platform, raw_message, pipeline_status, persisted_at FROM timeline_events WHERE pipeline_status = 'audit' ORDER BY persisted_at DESC LIMIT 40",
        )
        .await
    {
        if qr.success {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&qr.result_blob) {
                let rows = v
                    .as_array()
                    .map(|a| a.iter().map(row_from_json).collect())
                    .unwrap_or_default();
                let mut d = data.lock().await;
                d.audit = rows;
            }
        }
    }

    // Recent archival rows (module connect/disconnect, module logs, sends).
    if let Ok(qr) = engine
        .db_query(
            "audit_logs",
            "SELECT raw_message, flags, persisted_at FROM timeline_events WHERE event_type = 5 ORDER BY persisted_at DESC LIMIT 40",
        )
        .await
    {
        if qr.success {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&qr.result_blob) {
                if let Some(arr) = v.as_array() {
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
                        lines.push(format!("[{} {}] {}", age, if kind.is_empty() { "archive" } else { &kind }, msg));
                    }
                    let mut d = data.lock().await;
                    d.logs = lines;
                }
            }
        }
    }

    // Module list (virtual query).
    if let Ok(qr) = engine.db_query("module_list", "SELECT 1").await {
        if qr.success {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&qr.result_blob) {
                let mut d = data.lock().await;
                d.modules = v.as_array().cloned().unwrap_or_default();
                d.connected = true;
                d.last_refresh = Some(rel_time(Some(now_ms())));
            }
        }
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
                            refresh(&engine, &data).await;
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

    let status = if d.connected { "connected" } else { "connecting" };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " COCKATIEL AUDIT VIEWER ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" · {}", status),
            Style::default().fg(if d.connected { Color::Green } else { Color::Yellow }),
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
        Tab::Logs => d
            .logs
            .iter()
            .map(|l| ListItem::new(Line::from(vec![Span::raw(truncate(l, 120))])))
            .collect(),
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