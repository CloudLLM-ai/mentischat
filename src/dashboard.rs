//! The operator's web dashboard at `http://127.0.0.1:9480/dashboard`: a live
//! view of the agents, the rooms (members, roles, key fingerprints, bans) and
//! the traffic, the same family as MentisDB's dashboard.
//!
//! It is an observer and an operator, never a participant: it holds no agent
//! key, so it cannot post, join, invite or ban (every room write is signed by
//! an agent's own key). What it can do needs no key: show or hide message
//! previews, and run the expiry sweep now.
//!
//! # Access
//!
//! The dashboard is served on the daemon's loopback listener, behind the same
//! loopback, `Host`, `Origin` and `Sec-Fetch-Site` checks as the rest of it.
//! With `MENTISCHAT_DASHBOARD_PIN` set, it asks for the PIN first: a form post
//! that answers with an `HttpOnly`, `SameSite=Strict` session cookie, kept in
//! memory for [`SESSION_TTL`]. Wrong PINs are rate limited per client and in
//! total. Without a PIN the dashboard still opens, but shows no message text
//! and refuses the operator actions.
//!
//! API calls answer `401` JSON when the session is missing, never a redirect,
//! so the page can send the browser to the login form. Every `POST` must come
//! from the dashboard's own origin (`Origin` or `Sec-Fetch-Site`) and API
//! posts must also carry `X-MentisChat-Dashboard: 1`, which a cross-site form
//! cannot add.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::bus::Bus;

/// Where the dashboard lives.
pub const DASHBOARD_PATH: &str = "/dashboard";
/// The session cookie's name.
pub const SESSION_COOKIE: &str = "mentischat_session";
/// The header every dashboard API post carries.
pub const CSRF_HEADER: &str = "x-mentischat-dashboard";
/// How long a session lasts.
pub const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
/// Wrong PINs one client may try per [`LIMIT_WINDOW`].
pub const PIN_ATTEMPTS_PER_CLIENT: usize = 5;
/// Wrong PINs everyone together may try per [`LIMIT_WINDOW`]; past it every
/// PIN attempt waits for the window to pass.
pub const PIN_ATTEMPTS_TOTAL: usize = 50;
/// The window of the PIN rate limits.
pub const LIMIT_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Most sessions kept at once; the oldest goes first.
const MAX_SESSIONS: usize = 64;
/// Most clients the PIN limiter tracks.
const MAX_TRACKED_CLIENTS: usize = 4096;
/// Most activity events one poll returns.
const MAX_ACTIVITY_PAGE: usize = 500;

const INDEX_HTML: &str = include_str!("dashboard_static/index.html");
const LOGIN_HTML: &str = include_str!("dashboard_static/login.html");

/// How the dashboard is set up.
#[derive(Debug, Clone, Default)]
pub struct DashboardConfig {
    /// The PIN; `None` (or empty) for a dashboard without one, which shows no
    /// message text and refuses operator actions.
    pub pin: Option<String>,
}

impl DashboardConfig {
    /// The dashboard set up from `MENTISCHAT_DASHBOARD_PIN` (trimmed; empty
    /// means none).
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> Self {
        Self {
            pin: var("MENTISCHAT_DASHBOARD_PIN")
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty()),
        }
    }
}

/// Wrong-PIN bookkeeping.
#[derive(Debug, Default)]
struct Limiter {
    /// Recent failures per client (an IPv4 address, or an IPv6 /64).
    clients: HashMap<IpAddr, Vec<Instant>>,
    /// Recent failures from everyone.
    total: Vec<Instant>,
}

impl Limiter {
    fn prune(&mut self, now: Instant) {
        let fresh = |t: &Instant| now.duration_since(*t) < LIMIT_WINDOW;
        self.total.retain(fresh);
        self.clients.retain(|_, times| {
            times.retain(fresh);
            !times.is_empty()
        });
    }

    /// How long `client` must wait, if it must.
    fn blocked(&mut self, client: IpAddr, now: Instant) -> Option<Duration> {
        self.prune(now);
        let wait = |times: &[Instant], max: usize| {
            (times.len() >= max)
                .then(|| {
                    times
                        .first()
                        .map(|t| LIMIT_WINDOW.saturating_sub(now.duration_since(*t)))
                })
                .flatten()
        };
        let own = self.clients.get(&client).map_or(&[][..], Vec::as_slice);
        wait(own, PIN_ATTEMPTS_PER_CLIENT).or_else(|| wait(&self.total, PIN_ATTEMPTS_TOTAL))
    }

    fn failed(&mut self, client: IpAddr, now: Instant) {
        if self.clients.len() >= MAX_TRACKED_CLIENTS && !self.clients.contains_key(&client) {
            if let Some(oldest) = self
                .clients
                .iter()
                .min_by_key(|(_, t)| t.last().copied())
                .map(|(ip, _)| *ip)
            {
                self.clients.remove(&oldest);
            }
        }
        self.clients.entry(client).or_default().push(now);
        self.total.push(now);
    }

    fn succeeded(&mut self, client: IpAddr) {
        self.clients.remove(&client);
    }
}

/// The dashboard's shared state.
pub struct Dashboard {
    bus: Arc<Bus>,
    pin: Option<String>,
    sessions: Mutex<HashMap<String, Instant>>,
    limiter: Mutex<Limiter>,
    previews: AtomicBool,
}

impl std::fmt::Debug for Dashboard {
    /// Never prints the PIN or session tokens.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dashboard")
            .field("pin", &self.pin.is_some())
            .field("sessions", &self.sessions.lock().len())
            .finish()
    }
}

type Ctx = Arc<Dashboard>;

impl Dashboard {
    /// A dashboard over `bus`. Previews start on when there is a PIN.
    pub fn new(bus: Arc<Bus>, config: DashboardConfig) -> Self {
        let pin = config.pin.filter(|p| !p.is_empty());
        Self {
            previews: AtomicBool::new(pin.is_some()),
            bus,
            pin,
            sessions: Mutex::new(HashMap::new()),
            limiter: Mutex::new(Limiter::default()),
        }
    }

    fn pin_matches(&self, given: &str) -> bool {
        let Some(pin) = &self.pin else { return true };
        // Compare every byte of the longer of the two, so the time taken does
        // not say how much of the PIN was right.
        let (a, b) = (pin.as_bytes(), given.as_bytes());
        let mut diff = a.len() ^ b.len();
        for i in 0..a.len().max(b.len()) {
            diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
        }
        diff == 0
    }

    fn new_session(&self) -> String {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut sessions = self.sessions.lock();
        let now = Instant::now();
        sessions.retain(|_, born| now.duration_since(*born) < SESSION_TTL);
        while sessions.len() >= MAX_SESSIONS {
            let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, born)| **born)
                .map(|(t, _)| t.clone())
            else {
                break;
            };
            sessions.remove(&oldest);
        }
        sessions.insert(token.clone(), now);
        token
    }

    /// Whether the request may see the dashboard: no PIN, or a live session.
    fn admitted(&self, headers: &HeaderMap) -> bool {
        if self.pin.is_none() {
            return true;
        }
        session_token(headers).is_some_and(|token| {
            self.sessions
                .lock()
                .get(&token)
                .is_some_and(|born| born.elapsed() < SESSION_TTL)
        })
    }

    fn previews_on(&self) -> bool {
        self.pin.is_some() && self.previews.load(Ordering::Relaxed)
    }
}

fn session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == SESSION_COOKIE)
        .map(|(_, value)| value.to_string())
}

/// The client a PIN failure is counted against: an IPv4 address, or the /64
/// of an IPv6 one.
fn client_key(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let mut seg = v6.segments();
                seg[4..].fill(0);
                IpAddr::V6(seg.into())
            }
        },
        v4 => v4,
    }
}

/// The dashboard's routes, to merge into the daemon's router (which adds the
/// loopback and browser checks).
pub fn router(bus: Arc<Bus>, config: DashboardConfig) -> Router {
    let ctx: Ctx = Arc::new(Dashboard::new(bus, config));
    let api = Router::new()
        .route("/overview", get(api_overview))
        .route("/activity", get(api_activity))
        .route("/previews", post(api_previews))
        .route("/sweep", post(api_sweep))
        .layer(middleware::from_fn_with_state(
            ctx.clone(),
            require_session_api,
        ));
    let page = Router::new()
        .route("/", get(index))
        .layer(middleware::from_fn_with_state(
            ctx.clone(),
            require_session_page,
        ));
    let dashboard = Router::new()
        .merge(page)
        .route("/login", get(login_page).post(login))
        .route("/logout", post(logout))
        .nest("/api", api)
        .layer(middleware::from_fn(same_origin_posts))
        .layer(middleware::from_fn(security_headers))
        .with_state(ctx);
    Router::new()
        .route(
            "/dashboard/",
            get(|| async { Redirect::to(DASHBOARD_PATH) }),
        )
        .nest(DASHBOARD_PATH, dashboard)
}

async fn security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    let set = |headers: &mut HeaderMap, name: header::HeaderName, value: &'static str| {
        headers.insert(name, HeaderValue::from_static(value));
    };
    set(
        headers,
        header::CONTENT_SECURITY_POLICY,
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; \
         object-src 'none'; form-action 'self'",
    );
    set(headers, header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    set(headers, header::X_FRAME_OPTIONS, "DENY");
    set(headers, header::REFERRER_POLICY, "same-origin");
    set(headers, header::CACHE_CONTROL, "no-store");
    response
}

/// Every `POST` must come from the dashboard's own origin: `Origin` naming
/// the `Host`, or `Sec-Fetch-Site: same-origin`; API posts must also carry
/// [`CSRF_HEADER`]. A loopback page on another port passes the daemon's
/// loopback checks, so this is what keeps it out.
async fn same_origin_posts(headers: HeaderMap, request: Request, next: Next) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let text = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let host = text("host").unwrap_or_default();
    let same_origin = match text("origin").filter(|o| *o != "null") {
        Some(origin) => origin
            .strip_prefix("http://")
            .or_else(|| origin.strip_prefix("https://"))
            .is_some_and(|authority| authority.eq_ignore_ascii_case(host)),
        None => text("sec-fetch-site") == Some("same-origin"),
    };
    let api = request.uri().path().starts_with("/api/");
    if !same_origin || (api && text(CSRF_HEADER) != Some("1")) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "cross_origin", "message": "the dashboard only takes posts from its own page" })),
        )
            .into_response();
    }
    next.run(request).await
}

async fn require_session_api(
    State(ctx): State<Ctx>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if ctx.admitted(&headers) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "login_required" })),
        )
            .into_response()
    }
}

async fn require_session_page(
    State(ctx): State<Ctx>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if ctx.admitted(&headers) {
        next.run(request).await
    } else {
        Redirect::to("/dashboard/login").into_response()
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn login_page(State(ctx): State<Ctx>) -> Response {
    if ctx.pin.is_none() {
        return Redirect::to(DASHBOARD_PATH).into_response();
    }
    Html(LOGIN_HTML).into_response()
}

#[derive(Deserialize)]
struct LoginForm {
    pin: String,
}

async fn login(
    State(ctx): State<Ctx>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<LoginForm>,
) -> Response {
    let client = client_key(peer.ip());
    let now = Instant::now();
    if let Some(wait) = ctx.limiter.lock().blocked(client, now) {
        let secs = wait.as_secs().max(1);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, secs.to_string())],
            Html(format!(
                "<!doctype html><title>Too many attempts</title><p>Too many wrong PINs. Try again in {secs} s.</p>"
            )),
        )
            .into_response();
    }
    if !ctx.pin_matches(&form.pin) {
        ctx.limiter.lock().failed(client, now);
        return Redirect::to("/dashboard/login?error=1").into_response();
    }
    ctx.limiter.lock().succeeded(client);
    let token = ctx.new_session();
    let cookie = format!(
        "{SESSION_COOKIE}={token}; Path=/dashboard; HttpOnly; SameSite=Strict; Max-Age={}",
        SESSION_TTL.as_secs()
    );
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, DASHBOARD_PATH.to_string()),
            (header::SET_COOKIE, cookie),
        ],
    )
        .into_response()
}

async fn logout(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if let Some(token) = session_token(&headers) {
        ctx.sessions.lock().remove(&token);
    }
    let cookie =
        format!("{SESSION_COOKIE}=; Path=/dashboard; HttpOnly; SameSite=Strict; Max-Age=0");
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, "/dashboard/login".to_string()),
            (header::SET_COOKIE, cookie),
        ],
    )
        .into_response()
}

fn error(e: &crate::errors::MailboxError) -> Response {
    (e.http_status(), Json(e.to_json())).into_response()
}

/// `GET /dashboard/api/overview`: agents, rooms, counts and the dashboard's
/// own settings.
async fn api_overview(State(ctx): State<Ctx>) -> Response {
    match ctx.bus.operator_view().await {
        Ok(view) => {
            let mut body = serde_json::to_value(&view).unwrap_or(Value::Null);
            body["dashboard"] = json!({
                "pin": ctx.pin.is_some(),
                "previews": ctx.previews_on(),
                "version": env!("CARGO_PKG_VERSION"),
                "retention": {
                    "readTtlSecs": ctx.bus.retention().read_ttl.as_secs(),
                    "agentTtlSecs": ctx.bus.retention().agent_ttl.as_secs(),
                },
            });
            Json(body).into_response()
        }
        Err(e) => error(&e),
    }
}

#[derive(Deserialize)]
struct ActivityQuery {
    after: Option<u64>,
}

/// `GET /dashboard/api/activity?after=SEQ`: what was sent and read since
/// `after`, oldest first, with previews only while they are on.
async fn api_activity(State(ctx): State<Ctx>, Query(q): Query<ActivityQuery>) -> Response {
    let (mut events, latest) = ctx.bus.activity(q.after.unwrap_or(0), MAX_ACTIVITY_PAGE);
    if !ctx.previews_on() {
        for event in &mut events {
            event.preview = None;
        }
    }
    Json(json!({ "events": events, "latest": latest, "previews": ctx.previews_on() }))
        .into_response()
}

fn pin_required() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "pin_required",
            "message": "set MENTISCHAT_DASHBOARD_PIN to use the operator actions",
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
struct PreviewsBody {
    on: bool,
}

/// `POST /dashboard/api/previews {"on": bool}`: show or hide message text in
/// the activity feed (a dashboard with a PIN only).
async fn api_previews(State(ctx): State<Ctx>, Json(body): Json<PreviewsBody>) -> Response {
    if ctx.pin.is_none() {
        return pin_required();
    }
    ctx.previews.store(body.on, Ordering::Relaxed);
    Json(json!({ "previews": ctx.previews_on() })).into_response()
}

/// `POST /dashboard/api/sweep`: run the expiry sweep now (read mail past its
/// time, and idle agents with the mail addressed to them), as the timer
/// would.
async fn api_sweep(State(ctx): State<Ctx>) -> Response {
    if ctx.pin.is_none() {
        return pin_required();
    }
    match ctx.bus.sweep_at(std::time::SystemTime::now()).await {
        Ok(outcome) => Json(json!({
            "expiredRead": outcome.expired_read,
            "discarded": outcome.discarded,
            "droppedAgents": outcome.dropped_agents,
        }))
        .into_response(),
        Err(e) => error(&e),
    }
}
