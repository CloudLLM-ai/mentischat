//! The operator's web dashboard at `http://127.0.0.1:9480/dashboard`: a live
//! view of the agents, the rooms (members, roles, key fingerprints, bans) and
//! the traffic, the same family as MentisDB's dashboard, and the console the
//! operator runs rooms from.
//!
//! # The operator agent
//!
//! Every room write is signed by an agent's own key, and the daemon holds no
//! agent's key. The dashboard acts as an agent of its own instead, `operator`
//! (or `MENTISCHAT_OPERATOR`), whose key the daemon keeps in
//! `<data dir>/operator.key` (see [`operator`]). From the dashboard the
//! operator creates public and private rooms, joins, leaves, posts, invites,
//! kicks, bans and unbans, promotes and demotes, and sends signed direct
//! mail, under the same rules as any agent: it is admin of the rooms it
//! creates and of rooms whose admins promoted it, and nothing else. End-to-end
//! encrypted rooms are run by their members' clients; the dashboard shows
//! them, and only leaves them.
//!
//! # Access
//!
//! The dashboard is served on the daemon's loopback listener, behind the same
//! loopback, `Host`, `Origin` and `Sec-Fetch-Site` checks as the rest of it,
//! and always behind a PIN (see [`pin`]): `MENTISCHAT_DASHBOARD_PIN`, or the
//! one set on the dashboard the first time it is opened, which the settings
//! page can change. Logging in answers with an `HttpOnly`, `SameSite=Strict`
//! session cookie, kept in memory for [`SESSION_TTL`]. Wrong PINs are rate
//! limited per client and in total. Changing the PIN ends every other session.
//!
//! API calls answer `401` JSON when the session is missing (`login_required`,
//! or `setup_required` before a PIN is set), never a redirect, so the page can
//! send the browser to the right form. Every `POST` must come from the
//! dashboard's own origin (`Origin` or `Sec-Fetch-Site`) and API posts must
//! also carry `X-MentisChat-Dashboard: 1`, which a cross-site form cannot add.

pub mod operator;
pub mod pin;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::bus::{Bus, Message};
use crate::errors::MailboxError;
use crate::names::{AgentName, RoomName};
use crate::signed::{Envelope, PublicKey, SignedOp};

use operator::{Operator, DEFAULT_OPERATOR};
use pin::{Pin, PinRefused, PinSource};

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
const SETUP_HTML: &str = include_str!("dashboard_static/setup.html");

/// How the dashboard is set up.
#[derive(Debug, Clone, Default)]
pub struct DashboardConfig {
    /// The PIN from the environment; `None` (or empty) to use the one set on
    /// the dashboard, kept under [`dir`](Self::dir).
    pub pin: Option<String>,
    /// Where the PIN set on the dashboard and the operator's key are kept
    /// (the daemon's data directory). `None` keeps both in memory, for this
    /// process only.
    pub dir: Option<PathBuf>,
    /// The operator agent's name; `None` for [`DEFAULT_OPERATOR`].
    pub operator: Option<String>,
}

impl DashboardConfig {
    /// The dashboard set up from `MENTISCHAT_DASHBOARD_PIN` and
    /// `MENTISCHAT_OPERATOR` (both trimmed; empty means unset), keeping its
    /// files nowhere until [`with_dir`](Self::with_dir).
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> Self {
        let set = |name: &str| {
            var(name)
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
        };
        Self {
            pin: set("MENTISCHAT_DASHBOARD_PIN"),
            dir: None,
            operator: set("MENTISCHAT_OPERATOR"),
        }
    }

    /// Keep the dashboard's PIN and the operator's key in `dir`.
    #[must_use]
    pub fn with_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    /// The operator's name, checked.
    ///
    /// # Errors
    ///
    /// `invalid_name` when `MENTISCHAT_OPERATOR` is not a valid agent name.
    pub fn operator_name(&self) -> Result<AgentName, MailboxError> {
        AgentName::parse(self.operator.as_deref().unwrap_or(DEFAULT_OPERATOR))
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
    pin: RwLock<Pin>,
    operator: Result<Operator, String>,
    sessions: Mutex<HashMap<String, Instant>>,
    limiter: Mutex<Limiter>,
    previews: AtomicBool,
}

impl std::fmt::Debug for Dashboard {
    /// Never prints the PIN or session tokens.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dashboard")
            .field("pin", &self.pin.read().source())
            .field("operator", &self.operator)
            .field("sessions", &self.sessions.lock().len())
            .finish()
    }
}

type Ctx = Arc<Dashboard>;

impl Dashboard {
    /// A dashboard over `bus`. Previews start on.
    pub fn new(bus: Arc<Bus>, config: DashboardConfig) -> Self {
        let pin = Pin::load(
            config.pin.clone().filter(|p| !p.is_empty()),
            config.dir.as_deref(),
        );
        let operator = config
            .operator_name()
            .map_err(|e| e.to_string())
            .and_then(|name| {
                Operator::open(name, config.dir.as_deref()).map_err(|e| e.to_string())
            });
        if let Err(e) = &operator {
            tracing::error!(error = %e, "the dashboard's operator agent is unavailable");
        }
        Self {
            previews: AtomicBool::new(true),
            bus,
            pin: RwLock::new(pin),
            operator,
            sessions: Mutex::new(HashMap::new()),
            limiter: Mutex::new(Limiter::default()),
        }
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

    /// Whether the request may see the dashboard: a PIN is set and the
    /// request carries a live session.
    fn admitted(&self, headers: &HeaderMap) -> bool {
        self.pin.read().source() != PinSource::Unset
            && session_token(headers).is_some_and(|token| {
                self.sessions
                    .lock()
                    .get(&token)
                    .is_some_and(|born| born.elapsed() < SESSION_TTL)
            })
    }

    fn previews_on(&self) -> bool {
        self.previews.load(Ordering::Relaxed)
    }

    fn operator(&self) -> Result<&Operator, Box<Response>> {
        self.operator.as_ref().map_err(|e| {
            Box::new(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "operator_unavailable", "message": e })),
                )
                    .into_response(),
            )
        })
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
        .route("/pin", post(api_pin))
        .route("/rooms", post(api_create_room))
        .route("/rooms/{room}/messages", get(api_room_messages))
        .route("/rooms/{room}/{action}", post(api_room_action))
        .route("/direct", post(api_direct))
        .route("/mail", get(api_mail))
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
        .route("/setup", get(setup_page).post(setup))
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
        return next.run(request).await;
    }
    let error = if ctx.pin.read().source() == PinSource::Unset {
        "setup_required"
    } else {
        "login_required"
    };
    (StatusCode::UNAUTHORIZED, Json(json!({ "error": error }))).into_response()
}

async fn require_session_page(
    State(ctx): State<Ctx>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if ctx.admitted(&headers) {
        next.run(request).await
    } else if ctx.pin.read().source() == PinSource::Unset {
        Redirect::to("/dashboard/setup").into_response()
    } else {
        Redirect::to("/dashboard/login").into_response()
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn login_page(State(ctx): State<Ctx>) -> Response {
    if ctx.pin.read().source() == PinSource::Unset {
        return Redirect::to("/dashboard/setup").into_response();
    }
    Html(LOGIN_HTML).into_response()
}

async fn setup_page(State(ctx): State<Ctx>) -> Response {
    if ctx.pin.read().source() != PinSource::Unset {
        return Redirect::to("/dashboard/login").into_response();
    }
    Html(SETUP_HTML).into_response()
}

/// A response that logs the browser in with a fresh session and sends it to
/// `location`.
fn session_response(ctx: &Dashboard, location: &str) -> Response {
    let token = ctx.new_session();
    let cookie = format!(
        "{SESSION_COOKIE}={token}; Path=/dashboard; HttpOnly; SameSite=Strict; Max-Age={}",
        SESSION_TTL.as_secs()
    );
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, location.to_string()),
            (header::SET_COOKIE, cookie),
        ],
    )
        .into_response()
}

#[derive(Deserialize)]
struct LoginForm {
    pin: String,
}

/// Check `given` against the PIN off the async workers: a hashed PIN takes
/// thousands of SHA-256 rounds.
async fn pin_matches(ctx: &Ctx, given: String) -> bool {
    let ctx = ctx.clone();
    tokio::task::spawn_blocking(move || ctx.pin.read().matches(&given))
        .await
        .unwrap_or(false)
}

fn too_many(wait: Duration) -> Response {
    let secs = wait.as_secs().max(1);
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, secs.to_string())],
        Html(format!(
            "<!doctype html><title>Too many attempts</title><p>Too many wrong PINs. Try again in {secs} s.</p>"
        )),
    )
        .into_response()
}

async fn login(
    State(ctx): State<Ctx>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<LoginForm>,
) -> Response {
    if ctx.pin.read().source() == PinSource::Unset {
        return Redirect::to("/dashboard/setup").into_response();
    }
    let client = client_key(peer.ip());
    let now = Instant::now();
    if let Some(wait) = ctx.limiter.lock().blocked(client, now) {
        return too_many(wait);
    }
    if !pin_matches(&ctx, form.pin).await {
        ctx.limiter.lock().failed(client, now);
        return Redirect::to("/dashboard/login?error=1").into_response();
    }
    ctx.limiter.lock().succeeded(client);
    session_response(&ctx, DASHBOARD_PATH)
}

#[derive(Deserialize)]
struct SetupForm {
    pin: String,
    confirm: String,
}

/// `POST /dashboard/setup`: set the first PIN (only while there is none) and
/// log in with it.
async fn setup(State(ctx): State<Ctx>, Form(form): Form<SetupForm>) -> Response {
    if form.pin != form.confirm {
        return Redirect::to("/dashboard/setup?error=mismatch").into_response();
    }
    let set = {
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || {
            let mut pin = ctx.pin.write();
            if pin.source() != PinSource::Unset {
                return Err(None);
            }
            pin.set(&form.pin).map_err(Some)
        })
        .await
        .unwrap_or(Err(Some(PinRefused::Storage)))
    };
    match set {
        Ok(()) => {
            tracing::info!("dashboard PIN set");
            session_response(&ctx, DASHBOARD_PATH)
        }
        Err(None) => Redirect::to("/dashboard/login").into_response(),
        Err(Some(PinRefused::Length)) => {
            Redirect::to("/dashboard/setup?error=length").into_response()
        }
        Err(Some(_)) => Redirect::to("/dashboard/setup?error=storage").into_response(),
    }
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

fn error(e: &MailboxError) -> Response {
    (e.http_status(), Json(e.to_json())).into_response()
}

fn refuse(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({ "error": code, "message": message }))).into_response()
}

/// `GET /dashboard/api/overview`: agents, rooms, counts, the operator agent
/// and the dashboard's own settings.
async fn api_overview(State(ctx): State<Ctx>) -> Response {
    // An open dashboard is what the activity feed records for.
    ctx.bus.watch_activity();
    match ctx.bus.operator_view().await {
        Ok(view) => {
            let mut body = serde_json::to_value(&view).unwrap_or(Value::Null);
            let operator = ctx.operator.as_ref().ok().map(|op| {
                let key = op.public_key();
                json!({
                    "name": op.name().as_str(),
                    "publicKey": key.to_hex(),
                    "fingerprint": key.fingerprint(),
                })
            });
            body["dashboard"] = json!({
                "pinSource": match ctx.pin.read().source() {
                    PinSource::Env => "env",
                    PinSource::Dashboard => "dashboard",
                    PinSource::Unset => "unset",
                },
                "previews": ctx.previews_on(),
                "operator": operator,
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

#[derive(Deserialize)]
struct PreviewsBody {
    on: bool,
}

/// `POST /dashboard/api/previews {"on": bool}`: show or hide message text in
/// the activity feed.
async fn api_previews(State(ctx): State<Ctx>, Json(body): Json<PreviewsBody>) -> Response {
    ctx.previews.store(body.on, Ordering::Relaxed);
    Json(json!({ "previews": ctx.previews_on() })).into_response()
}

/// `POST /dashboard/api/sweep`: run the expiry sweep now (read mail past its
/// time, and idle agents with the mail addressed to them), as the timer
/// would.
async fn api_sweep(State(ctx): State<Ctx>) -> Response {
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

#[derive(Deserialize)]
struct PinBody {
    current: String,
    new: String,
}

/// `POST /dashboard/api/pin {"current", "new"}`: change a PIN set on the
/// dashboard. Every other session ends; this one gets a fresh cookie.
async fn api_pin(
    State(ctx): State<Ctx>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(body): Json<PinBody>,
) -> Response {
    if ctx.pin.read().source() == PinSource::Env {
        return refuse(
            StatusCode::CONFLICT,
            "pin_from_env",
            "MENTISCHAT_DASHBOARD_PIN sets the PIN; change it there",
        );
    }
    let client = client_key(peer.ip());
    let now = Instant::now();
    if let Some(wait) = ctx.limiter.lock().blocked(client, now) {
        let secs = wait.as_secs().max(1);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, secs.to_string())],
            Json(json!({ "error": "rate_limited", "retryAfterSecs": secs })),
        )
            .into_response();
    }
    if !pin_matches(&ctx, body.current).await {
        ctx.limiter.lock().failed(client, now);
        return refuse(
            StatusCode::FORBIDDEN,
            "wrong_pin",
            "the current PIN is wrong",
        );
    }
    let set = {
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || ctx.pin.write().set(&body.new))
            .await
            .unwrap_or(Err(PinRefused::Storage))
    };
    match set {
        Ok(()) => {
            ctx.sessions.lock().clear();
            tracing::info!("dashboard PIN changed; other sessions ended");
            let token = ctx.new_session();
            let cookie = format!(
                "{SESSION_COOKIE}={token}; Path=/dashboard; HttpOnly; SameSite=Strict; Max-Age={}",
                SESSION_TTL.as_secs()
            );
            (
                [(header::SET_COOKIE, cookie)],
                Json(json!({ "changed": true })),
            )
                .into_response()
        }
        Err(PinRefused::Length) => refuse(
            StatusCode::BAD_REQUEST,
            "invalid_pin",
            &format!(
                "a PIN has {} to {} characters",
                pin::MIN_PIN_CHARS,
                pin::MAX_PIN_CHARS
            ),
        ),
        Err(PinRefused::FromEnv) => refuse(
            StatusCode::CONFLICT,
            "pin_from_env",
            "MENTISCHAT_DASHBOARD_PIN sets the PIN; change it there",
        ),
        Err(PinRefused::Storage) => refuse(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage",
            "the PIN could not be saved",
        ),
    }
}

#[derive(Deserialize)]
struct CreateBody {
    room: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    public: bool,
}

/// `POST /dashboard/api/rooms {"room", "description"?, "public"?}`: create a
/// signed room with the operator as its admin.
async fn api_create_room(State(ctx): State<Ctx>, Json(body): Json<CreateBody>) -> Response {
    let op = match ctx.operator() {
        Ok(op) => op,
        Err(r) => return *r,
    };
    let room = match RoomName::parse(&body.room) {
        Ok(room) => room,
        Err(e) => return error(&e),
    };
    let create = SignedOp::Create {
        description: body.description.trim().to_string(),
        public: body.public,
    };
    match op.act(&ctx.bus, &room, create).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => error(&e),
    }
}

/// The key bound to `agent`, if any.
async fn bound_key(bus: &Bus, agent: &AgentName) -> Result<Option<PublicKey>, MailboxError> {
    Ok(bus
        .agent_key(agent)
        .await?
        .and_then(|b| b.public_key.parse().ok()))
}

/// The `agent` field of an action's body.
fn target(body: &Value) -> Result<AgentName, MailboxError> {
    AgentName::parse(
        body.get("agent")
            .and_then(Value::as_str)
            .ok_or(MailboxError::InvalidBody)?,
    )
}

/// `POST /dashboard/api/rooms/{room}/{action}`: one room action as the
/// operator. `join`, `leave`; `send` with `{"text"}`; `invite`, `kick`,
/// `ban`, `unban`, `promote`, `demote` with `{"agent"}`. In an end-to-end
/// encrypted room only `leave`, `promote` and `demote` (which change no
/// group) are taken: the rest belong to the members' clients.
async fn api_room_action(
    State(ctx): State<Ctx>,
    Path((room, action)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Response {
    let op = match ctx.operator() {
        Ok(op) => op,
        Err(r) => return *r,
    };
    let body = body.map(|Json(v)| v).unwrap_or(Value::Null);
    let room = match RoomName::parse(&room) {
        Ok(room) => room,
        Err(e) => return error(&e),
    };
    let encrypted = match ctx.bus.room_encrypted(&room) {
        Ok(Some(encrypted)) => encrypted,
        Ok(None) => {
            return error(&MailboxError::RoomNotFound {
                room: room.to_string(),
            })
        }
        Err(e) => return error(&e),
    };
    if encrypted && !matches!(action.as_str(), "leave" | "promote" | "demote") {
        return refuse(
            StatusCode::CONFLICT,
            "secure_room",
            "an end-to-end encrypted room's group is run by its members' clients",
        );
    }
    let signed = async {
        let named = || target(&body);
        Ok::<_, MailboxError>(match action.as_str() {
            "join" => SignedOp::Join,
            "leave" => SignedOp::Leave,
            "send" => SignedOp::Send {
                body: body
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(MailboxError::InvalidBody)?
                    .to_string(),
            },
            "invite" => {
                let invitee = named()?;
                let invitee_key = bound_key(&ctx.bus, &invitee).await?.ok_or_else(|| {
                    MailboxError::KeyRequired {
                        agent: invitee.to_string(),
                    }
                })?;
                SignedOp::Invite {
                    invitee: invitee.to_string(),
                    invitee_key,
                }
            }
            "kick" => SignedOp::Remove {
                target: named()?.to_string(),
            },
            "ban" => {
                let banned = named()?;
                let target_key = match bound_key(&ctx.bus, &banned).await {
                    Ok(key) => key,
                    Err(MailboxError::Unregistered { .. }) => None,
                    Err(e) => return Err(e),
                };
                SignedOp::Ban {
                    target: banned.to_string(),
                    target_key,
                }
            }
            "unban" => SignedOp::Unban {
                target: named()?.to_string(),
            },
            "promote" => SignedOp::Promote {
                target: named()?.to_string(),
            },
            "demote" => SignedOp::Demote {
                target: named()?.to_string(),
            },
            _ => return Err(MailboxError::NotFound),
        })
    }
    .await;
    let signed = match signed {
        Ok(signed) => signed,
        Err(e) => return error(&e),
    };
    match op.act(&ctx.bus, &room, signed).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => error(&e),
    }
}

/// One message as the dashboard shows it: the text out of its envelope, and
/// whether the envelope's signature holds.
fn shown(message: &Message, verified: impl Fn(&Envelope<'_>) -> bool) -> Value {
    let envelope = Envelope::parse(&message.body).ok();
    let encrypted = envelope
        .as_ref()
        .is_some_and(|e| e.body.starts_with("mls/"));
    let text = match &envelope {
        Some(_) if encrypted => None,
        Some(e) => Some(e.body),
        None => Some(message.body.as_str()),
    };
    json!({
        "id": message.id,
        "from": message.from,
        "to": (!message.to.is_empty()).then_some(&message.to),
        "room": message.room,
        "createdAt": message.created_at,
        "text": text,
        "signed": envelope.is_some(),
        "verified": envelope.as_ref().is_some_and(&verified),
        "encrypted": encrypted,
    })
}

#[derive(Deserialize)]
struct BeforeQuery {
    before: Option<i64>,
}

/// `GET /dashboard/api/rooms/{room}/messages?before=`: the room's messages as
/// the operator, a member, sees them; `not_a_member` otherwise.
async fn api_room_messages(
    State(ctx): State<Ctx>,
    Path(room): Path<String>,
    Query(q): Query<BeforeQuery>,
) -> Response {
    let op = match ctx.operator() {
        Ok(op) => op,
        Err(r) => return *r,
    };
    let room = match RoomName::parse(&room) {
        Ok(room) => room,
        Err(e) => return error(&e),
    };
    if let Err(e) = op.present(&ctx.bus).await {
        return error(&e);
    }
    match ctx.bus.room_history(op.name(), &room, q.before).await {
        Ok(page) => {
            let messages: Vec<Value> = page
                .messages
                .iter()
                .map(|m| shown(m, |e| e.verify(room.as_str(), &m.from).is_ok()))
                .collect();
            Json(json!({ "room": room.as_str(), "messages": messages, "next": page.next }))
                .into_response()
        }
        Err(e) => error(&e),
    }
}

#[derive(Deserialize)]
struct DirectBody {
    to: String,
    text: String,
}

/// `POST /dashboard/api/direct {"to", "text"}`: a signed direct message from
/// the operator.
async fn api_direct(State(ctx): State<Ctx>, Json(body): Json<DirectBody>) -> Response {
    let op = match ctx.operator() {
        Ok(op) => op,
        Err(r) => return *r,
    };
    let to = match AgentName::parse(&body.to) {
        Ok(to) => to,
        Err(e) => return error(&e),
    };
    match op.direct(&ctx.bus, &to, &body.text).await {
        Ok(receipt) => Json(receipt).into_response(),
        Err(e) => error(&e),
    }
}

/// `GET /dashboard/api/mail?before=`: the operator's direct mail, both ways,
/// newest page first; what was waiting is marked read. A message counts as
/// verified when its envelope's key is the one bound to its sender.
async fn api_mail(State(ctx): State<Ctx>, Query(q): Query<BeforeQuery>) -> Response {
    let op = match ctx.operator() {
        Ok(op) => op,
        Err(r) => return *r,
    };
    if let Err(e) = op.present(&ctx.bus).await {
        return error(&e);
    }
    // Take what is waiting, so it shows as read and stops counting as queued.
    loop {
        match ctx.bus.check_inbox(op.name(), 0).await {
            Ok(page) if !page.messages.is_empty() && page.remaining > 0 => continue,
            Ok(_) => break,
            Err(e) => return error(&e),
        }
    }
    let page = match ctx.bus.history(op.name(), None, q.before).await {
        Ok(page) => page,
        Err(e) => return error(&e),
    };
    let mut keys: HashMap<String, Option<PublicKey>> = HashMap::new();
    for m in &page.messages {
        if !keys.contains_key(&m.from) {
            let key = match AgentName::parse(&m.from) {
                Ok(name) => bound_key(&ctx.bus, &name).await.ok().flatten(),
                Err(_) => None,
            };
            keys.insert(m.from.clone(), key);
        }
    }
    let messages: Vec<Value> = page
        .messages
        .iter()
        .map(|m| {
            shown(m, |e| {
                keys.get(&m.from).copied().flatten() == Some(e.key)
                    && e.verify_direct(&m.from, &m.to).is_ok()
            })
        })
        .collect();
    Json(json!({ "agent": op.name().as_str(), "messages": messages, "next": page.next }))
        .into_response()
}
