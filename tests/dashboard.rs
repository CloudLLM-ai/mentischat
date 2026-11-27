//! The operator's web dashboard through the daemon's real router: the PIN
//! login, sessions and logout, the wrong-PIN limit, the same-origin rule for
//! posts, the security headers, the overview of agents and rooms (roles,
//! fingerprints, bans), the activity feed and its previews, the PIN set on
//! the first visit and changed later, and the operator agent's room actions.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::connect_info::MockConnectInfo;
use axum::Router;
use http_body_util::BodyExt;
use mentischat::bus::{Bus, SignedRequest};
use mentischat::dashboard::{DashboardConfig, PIN_ATTEMPTS_PER_CLIENT, SESSION_COOKIE};
use mentischat::signed::{direct_payload, register_key_payload, Keypair, SignedOp};
use serde_json::{json, Value};
use tower::ServiceExt;

const HOST: &str = "127.0.0.1:9480";
const ORIGIN: &str = "http://127.0.0.1:9480";

fn app_with(pin: Option<&str>) -> (tempfile::TempDir, Arc<Bus>, Router) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(
        Bus::new(&dir.path().join("dash"))
            .unwrap()
            .with_signatures_required(),
    );
    let router = router_over(&bus, dir.path(), pin);
    (dir, bus, router)
}

/// A dashboard over `bus` keeping its files in `dir/dash`, as the daemon
/// would after a restart.
fn router_over(bus: &Arc<Bus>, dir: &std::path::Path, pin: Option<&str>) -> Router {
    let config = DashboardConfig {
        pin: pin.map(str::to_string),
        dir: Some(dir.join("dash")),
        operator: None,
    };
    mentischat::build_router_with(bus.clone(), config)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))))
}

struct Reply {
    status: u16,
    headers: http::HeaderMap,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
    fn location(&self) -> &str {
        self.headers
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }
    fn cookie(&self) -> Option<String> {
        let set = self.headers.get("set-cookie")?.to_str().ok()?;
        let pair = set.split(';').next()?;
        pair.strip_prefix(&format!("{SESSION_COOKIE}="))
            .filter(|v| !v.is_empty())
            .map(|v| format!("{SESSION_COOKIE}={v}"))
    }
}

async fn send(app: &Router, req: http::Request<axum::body::Body>) -> Reply {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    Reply {
        status,
        headers,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn get(path: &str, cookie: Option<&str>) -> http::Request<axum::body::Body> {
    let mut b = http::Request::builder().uri(path).header("host", HOST);
    if let Some(c) = cookie {
        b = b.header("cookie", c);
    }
    b.body(axum::body::Body::empty()).unwrap()
}

fn post_json(
    path: &str,
    cookie: Option<&str>,
    body: Value,
    origin: Option<&str>,
    csrf: bool,
) -> http::Request<axum::body::Body> {
    let mut b = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", HOST)
        .header("content-type", "application/json");
    if let Some(c) = cookie {
        b = b.header("cookie", c);
    }
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    if csrf {
        b = b.header("x-mentischat-dashboard", "1");
    }
    b.body(axum::body::Body::from(body.to_string())).unwrap()
}

fn login_form(pin: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method("POST")
        .uri("/dashboard/login")
        .header("host", HOST)
        .header("origin", ORIGIN)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(axum::body::Body::from(format!("pin={pin}")))
        .unwrap()
}

async fn login(app: &Router) -> String {
    let r = send(app, login_form("4242")).await;
    assert_eq!(r.status, 303);
    assert_eq!(r.location(), "/dashboard");
    r.cookie().expect("a session cookie")
}

/// Vega and Orion with keys; Vega founds the public room `lobby` and Orion
/// joins; Vega sends Orion a signed direct message and Orion reads it.
async fn traffic(bus: &Bus) {
    let (vega, orion) = (Keypair::from_seed([1; 32]), Keypair::from_seed([2; 32]));
    for (name, k) in [("Vega", &vega), ("Orion", &orion)] {
        common::register(bus, &[name]).await;
        bus.register_key(
            &common::name(name),
            k.public(),
            &k.sign(&register_key_payload(name, &k.public())),
        )
        .await
        .unwrap();
    }
    let op = |k: &Keypair, who: &str, counter, op: SignedOp| SignedRequest {
        agent: common::name(who),
        room: common::room("lobby"),
        counter,
        signature: k.sign(&op.payload("lobby", who, counter)),
        op,
    };
    let create = SignedOp::Create {
        description: "the lobby".into(),
        public: true,
    };
    bus.signed_op(op(&vega, "Vega", 1, create)).await.unwrap();
    bus.signed_op(op(&orion, "Orion", 1, SignedOp::Join))
        .await
        .unwrap();
    bus.signed_op(op(
        &vega,
        "Vega",
        2,
        SignedOp::Send {
            body: "mls/1 app 0 beef".into(),
        },
    ))
    .await
    .unwrap();
    let body = "hello Orion\nsecond line";
    bus.send_signed(
        &common::name("Vega"),
        &common::name("Orion"),
        body.into(),
        1,
        &vega.sign(&direct_payload("Vega", "Orion", 1, body)),
    )
    .await
    .unwrap();
    bus.check_inbox(&common::name("Orion"), 0).await.unwrap();
}

/// With a PIN, the page sends the browser to the login form, the API answers
/// 401 JSON, a wrong PIN goes back to the form, and the right one opens a
/// session cookie that is `HttpOnly` and `SameSite=Strict`.
#[tokio::test]
async fn the_pin_guards_the_page_and_the_api() {
    let (_d, _bus, app) = app_with(Some("4242"));
    let r = send(&app, get("/dashboard", None)).await;
    assert_eq!((r.status, r.location()), (303, "/dashboard/login"));
    let r = send(&app, get("/dashboard/api/overview", None)).await;
    assert_eq!(r.status, 401);
    assert_eq!(r.json()["error"], "login_required");
    let r = send(&app, get("/dashboard/login", None)).await;
    assert_eq!(r.status, 200);
    assert!(r.body.contains("name=\"pin\""));

    let r = send(&app, login_form("1111")).await;
    assert_eq!((r.status, r.location()), (303, "/dashboard/login?error=1"));
    assert!(r.cookie().is_none());

    let r = send(&app, login_form("4242")).await;
    let set = r.headers["set-cookie"].to_str().unwrap().to_string();
    assert!(
        set.contains("HttpOnly") && set.contains("SameSite=Strict"),
        "{set}"
    );
    let cookie = r.cookie().unwrap();
    let r = send(&app, get("/dashboard", Some(&cookie))).await;
    assert_eq!(r.status, 200);
    assert!(r.body.contains("MentisChat"));
    let r = send(&app, get("/dashboard/api/overview", Some(&cookie))).await;
    assert_eq!(r.status, 200);
    let r = send(
        &app,
        get("/dashboard/api/overview", Some("mentischat_session=forged")),
    )
    .await;
    assert_eq!(r.status, 401);
    let r = send(&app, get("/dashboard/", None)).await;
    assert_eq!((r.status, r.location()), (303, "/dashboard"));
}

/// Logging out ends the session on the server, not only in the browser.
#[tokio::test]
async fn logout_ends_the_session() {
    let (_d, _bus, app) = app_with(Some("4242"));
    let cookie = login(&app).await;
    let r = send(
        &app,
        post_json(
            "/dashboard/logout",
            Some(&cookie),
            json!({}),
            Some(ORIGIN),
            false,
        ),
    )
    .await;
    assert_eq!((r.status, r.location()), (303, "/dashboard/login"));
    let r = send(&app, get("/dashboard/api/overview", Some(&cookie))).await;
    assert_eq!(r.status, 401);
}

/// After a few wrong PINs a client waits, even with the right PIN, and is
/// told how long.
#[tokio::test]
async fn wrong_pins_are_limited() {
    let (_d, _bus, app) = app_with(Some("4242"));
    for _ in 0..PIN_ATTEMPTS_PER_CLIENT {
        assert_eq!(send(&app, login_form("0000")).await.status, 303);
    }
    let r = send(&app, login_form("4242")).await;
    assert_eq!(r.status, 429);
    let wait: u64 = r.headers["retry-after"].to_str().unwrap().parse().unwrap();
    assert!(wait > 0);
}

/// Posts must come from the dashboard's own page: the right `Origin` (or
/// `Sec-Fetch-Site: same-origin`), and for the API the dashboard header too. A
/// login form posted from another local port is refused.
#[tokio::test]
async fn posts_must_come_from_the_dashboard_itself() {
    let (_d, _bus, app) = app_with(Some("4242"));
    let cookie = login(&app).await;
    let on = json!({ "on": false });
    let cases = [
        (Some(ORIGIN), false, 403),
        (Some("http://127.0.0.1:3000"), true, 403),
        (Some("http://localhost:9480"), true, 403),
        (None, true, 403),
        (Some(ORIGIN), true, 200),
    ];
    for (origin, csrf, want) in cases {
        let r = send(
            &app,
            post_json(
                "/dashboard/api/previews",
                Some(&cookie),
                on.clone(),
                origin,
                csrf,
            ),
        )
        .await;
        assert_eq!(r.status, want, "{origin:?} csrf={csrf}");
    }
    let mut req = post_json("/dashboard/api/previews", Some(&cookie), on, None, true);
    req.headers_mut()
        .insert("sec-fetch-site", "same-origin".parse().unwrap());
    assert_eq!(send(&app, req).await.status, 200);

    let mut form = login_form("4242");
    form.headers_mut()
        .insert("origin", "http://127.0.0.1:3000".parse().unwrap());
    assert_eq!(send(&app, form).await.status, 403);
}

/// Every dashboard response carries the security headers and is not cached.
#[tokio::test]
async fn responses_carry_security_headers() {
    let (_d, _bus, app) = app_with(Some("4242"));
    let cookie = login(&app).await;
    for path in ["/dashboard", "/dashboard/login", "/dashboard/api/overview"] {
        let r = send(&app, get(path, Some(&cookie))).await;
        let h = |name: &str| {
            r.headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
        };
        assert!(
            h("content-security-policy").contains("frame-ancestors 'none'"),
            "{path}"
        );
        assert_eq!(h("x-frame-options"), "DENY", "{path}");
        assert_eq!(h("x-content-type-options"), "nosniff", "{path}");
        assert_eq!(h("cache-control"), "no-store", "{path}");
    }
}

/// The overview shows every agent with its key fingerprint and every room
/// with its members' roles and fingerprints; the activity feed shows what was
/// sent and read, with the text of signed messages but never of encrypted ones.
#[tokio::test]
async fn the_overview_and_activity_show_the_mailbox() {
    let (_d, bus, app) = app_with(Some("4242"));
    // The feed records only while the dashboard is open.
    bus.watch_activity();
    traffic(&bus).await;
    let cookie = login(&app).await;
    let o = send(&app, get("/dashboard/api/overview", Some(&cookie)))
        .await
        .json();
    assert_eq!(o["agentsTotal"], 2);
    assert_eq!(o["signaturesRequired"], true);
    assert_eq!(o["dashboard"]["pinSource"], "env");
    assert_eq!(o["dashboard"]["previews"], true);
    assert_eq!(o["dashboard"]["operator"]["name"], "operator");
    let vega = Keypair::from_seed([1; 32]).public();
    let row = o["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "Vega")
        .unwrap();
    assert_eq!(row["fingerprint"], vega.fingerprint());
    assert_eq!(row["rooms"], 1);
    let lobby = &o["rooms"][0];
    assert_eq!(lobby["room"], "lobby");
    assert_eq!(lobby["public"], true);
    assert_eq!(lobby["signed"], true);
    assert_eq!(lobby["members"][1]["agent"], "Vega");
    assert_eq!(lobby["members"][1]["role"], "admin");
    assert_eq!(lobby["members"][0]["role"], "participant");
    assert_eq!(lobby["members"][1]["fingerprint"], vega.fingerprint());

    let a = send(&app, get("/dashboard/api/activity?after=0", Some(&cookie)))
        .await
        .json();
    let events = a["events"].as_array().unwrap();
    let sent: Vec<&Value> = events.iter().filter(|e| e["kind"] == "sent").collect();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["room"], "lobby");
    assert_eq!(sent[0]["encrypted"], true);
    assert!(
        sent[0].get("preview").is_none(),
        "ciphertext is never shown"
    );
    assert_eq!(sent[1]["to"], "Orion");
    assert_eq!(sent[1]["signed"], true);
    assert_eq!(sent[1]["preview"], "hello Orion…");
    let read = events.iter().find(|e| e["kind"] == "read").unwrap();
    assert_eq!(read["reader"], "Orion");
    assert!(
        !a.to_string().contains("mentischat-signed/1"),
        "no envelopes"
    );

    // Paging: nothing after the latest.
    let latest = a["latest"].as_u64().unwrap();
    let a = send(
        &app,
        get(
            &format!("/dashboard/api/activity?after={latest}"),
            Some(&cookie),
        ),
    )
    .await
    .json();
    assert!(a["events"].as_array().unwrap().is_empty());

    // Previews off: the text goes, the rest stays.
    let r = send(
        &app,
        post_json(
            "/dashboard/api/previews",
            Some(&cookie),
            json!({ "on": false }),
            Some(ORIGIN),
            true,
        ),
    )
    .await;
    assert_eq!(r.json()["previews"], false);
    let a = send(&app, get("/dashboard/api/activity?after=0", Some(&cookie)))
        .await
        .json();
    assert!(!a.to_string().contains("hello Orion"));
    assert_eq!(a["events"].as_array().unwrap().len(), events.len());
}

fn setup_form(pin: &str, confirm: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method("POST")
        .uri("/dashboard/setup")
        .header("host", HOST)
        .header("origin", ORIGIN)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(axum::body::Body::from(format!(
            "pin={pin}&confirm={confirm}"
        )))
        .unwrap()
}

/// Without `MENTISCHAT_DASHBOARD_PIN`, the first visit sets the PIN: until
/// then every page leads to the setup form and the API answers
/// `setup_required`. The PIN is stored hashed, `0600`, survives a restart,
/// and cannot be set a second time through the form.
#[tokio::test]
async fn the_first_visit_sets_the_pin() {
    let (dir, bus, app) = app_with(None);
    for path in ["/dashboard", "/dashboard/login"] {
        let r = send(&app, get(path, None)).await;
        assert_eq!(
            (r.status, r.location()),
            (303, "/dashboard/setup"),
            "{path}"
        );
    }
    let r = send(&app, get("/dashboard/api/overview", None)).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (401, json!("setup_required"))
    );
    let r = send(&app, get("/dashboard/setup", None)).await;
    assert!(r.status == 200 && r.body.contains("name=\"confirm\""));
    let r = send(&app, login_form("anything")).await;
    assert_eq!(r.location(), "/dashboard/setup", "no PIN, no login");

    let r = send(&app, setup_form("hunter22", "hunter23")).await;
    assert_eq!(r.location(), "/dashboard/setup?error=mismatch");
    let r = send(&app, setup_form("12345", "12345")).await;
    assert_eq!(r.location(), "/dashboard/setup?error=length");
    let r = send(&app, setup_form("hunter22", "hunter22")).await;
    assert_eq!((r.status, r.location()), (303, "/dashboard"));
    let cookie = r.cookie().expect("logged in by setting it");
    let o = send(&app, get("/dashboard/api/overview", Some(&cookie)))
        .await
        .json();
    assert_eq!(o["dashboard"]["pinSource"], "dashboard");

    let file = dir
        .path()
        .join("dash")
        .join(mentischat::dashboard::pin::PIN_FILE);
    let text = std::fs::read_to_string(&file).unwrap();
    assert!(
        text.starts_with("v1 ") && !text.contains("hunter22"),
        "{text}"
    );
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);

    let r = send(&app, setup_form("override1", "override1")).await;
    assert_eq!(r.location(), "/dashboard/login", "set once");
    assert!(r.cookie().is_none());

    // A restarted daemon reads it back.
    let again = router_over(&bus, dir.path(), None);
    let r = send(&again, login_form("override1")).await;
    assert_eq!(r.location(), "/dashboard/login?error=1");
    let r = send(&again, login_form("hunter22")).await;
    assert_eq!(r.location(), "/dashboard");
}

/// The settings page changes a PIN set on the dashboard: the current one is
/// required, every other session ends, and the caller gets a fresh cookie. A
/// PIN from the environment cannot be changed there.
#[tokio::test]
async fn the_pin_can_be_changed_on_the_dashboard() {
    let (dir, bus, app) = app_with(None);
    let first = send(&app, setup_form("hunter22", "hunter22"))
        .await
        .cookie()
        .unwrap();
    let other = send(&app, login_form("hunter22")).await.cookie().unwrap();
    let change = |cookie: &str, current: &str, new: &str| {
        post_json(
            "/dashboard/api/pin",
            Some(cookie),
            json!({ "current": current, "new": new }),
            Some(ORIGIN),
            true,
        )
    };
    let r = send(&app, change(&first, "wrong-pin", "fresh-pin")).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (403, json!("wrong_pin"))
    );
    let r = send(&app, change(&first, "hunter22", "short")).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (400, json!("invalid_pin"))
    );
    let r = send(&app, change(&first, "hunter22", "fresh-pin")).await;
    assert_eq!(r.status, 200);
    let renewed = r.cookie().expect("a fresh session");
    for (cookie, status) in [(&first, 401), (&other, 401), (&renewed, 200)] {
        let r = send(&app, get("/dashboard/api/overview", Some(cookie))).await;
        assert_eq!(r.status, status);
    }
    assert_eq!(
        send(&app, login_form("hunter22")).await.location(),
        "/dashboard/login?error=1"
    );
    assert_eq!(
        send(&app, login_form("fresh-pin")).await.location(),
        "/dashboard"
    );

    let fixed = router_over(&bus, dir.path(), Some("4242"));
    let cookie = login(&fixed).await;
    let r = send(&fixed, change(&cookie, "4242", "fresh-pin2")).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (409, json!("pin_from_env"))
    );
}

async fn act(app: &Router, cookie: &str, path: &str, body: Value) -> Reply {
    send(
        app,
        post_json(
            &format!("/dashboard/api{path}"),
            Some(cookie),
            body,
            Some(ORIGIN),
            true,
        ),
    )
    .await
}

fn room_of<'a>(o: &'a Value, name: &str) -> &'a Value {
    o["rooms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["room"] == name)
        .unwrap_or_else(|| panic!("no room {name} in {o}"))
}

fn role_in(o: &Value, room: &str, agent: &str) -> Option<String> {
    room_of(o, room)["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["agent"] == agent)
        .map(|m| m["role"].as_str().unwrap().to_string())
}

/// The operator agent runs rooms from the dashboard under the room rules:
/// it creates a room and is its admin, promotes, demotes, kicks, bans (with
/// the key) and unbans, invites into a private room, posts signed messages
/// and reads them back verified, and sends and reads signed direct mail.
#[tokio::test]
async fn the_operator_runs_rooms_from_the_dashboard() {
    let (_d, bus, app) = app_with(Some("4242"));
    traffic(&bus).await;
    let cookie = login(&app).await;
    let overview = || async {
        send(&app, get("/dashboard/api/overview", Some(&cookie)))
            .await
            .json()
    };

    let r = act(
        &app,
        &cookie,
        "/rooms",
        json!({ "room": "ops", "description": "operations", "public": true }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let o = overview().await;
    assert_eq!(role_in(&o, "ops", "operator").as_deref(), Some("admin"));
    assert_eq!(room_of(&o, "ops")["public"], true);
    let op_fp = o["dashboard"]["operator"]["fingerprint"].clone();
    let me = o["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "operator")
        .unwrap();
    assert_eq!(me["fingerprint"], op_fp, "registered with its key");

    // Vega and Orion join the public room with their own keys.
    let (vega, orion) = (Keypair::from_seed([1; 32]), Keypair::from_seed([2; 32]));
    let say = |k: &Keypair, who: &str, room: &str, counter: u64, op: SignedOp| SignedRequest {
        agent: common::name(who),
        room: common::room(room),
        counter,
        signature: k.sign(&op.payload(room, who, counter)),
        op,
    };
    bus.signed_op(say(&vega, "Vega", "ops", 10, SignedOp::Join))
        .await
        .unwrap();
    bus.signed_op(say(&orion, "Orion", "ops", 10, SignedOp::Join))
        .await
        .unwrap();

    let r = act(
        &app,
        &cookie,
        "/rooms/ops/promote",
        json!({ "agent": "Vega" }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        role_in(&overview().await, "ops", "Vega").as_deref(),
        Some("admin")
    );
    act(
        &app,
        &cookie,
        "/rooms/ops/demote",
        json!({ "agent": "Vega" }),
    )
    .await;
    assert_eq!(
        role_in(&overview().await, "ops", "Vega").as_deref(),
        Some("participant")
    );

    let r = act(&app, &cookie, "/rooms/ops/ban", json!({ "agent": "Orion" })).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let o = overview().await;
    assert_eq!(role_in(&o, "ops", "Orion"), None);
    assert_eq!(
        room_of(&o, "ops")["banned"][0]["publicKey"],
        orion.public().to_hex()
    );
    let back = bus
        .signed_op(say(&orion, "Orion", "ops", 11, SignedOp::Join))
        .await;
    assert_eq!(back.unwrap_err().code(), "banned");
    act(
        &app,
        &cookie,
        "/rooms/ops/unban",
        json!({ "agent": "Orion" }),
    )
    .await;
    bus.signed_op(say(&orion, "Orion", "ops", 12, SignedOp::Join))
        .await
        .unwrap();
    act(
        &app,
        &cookie,
        "/rooms/ops/kick",
        json!({ "agent": "Orion" }),
    )
    .await;
    assert_eq!(role_in(&overview().await, "ops", "Orion"), None);

    // Posting, and reading the room back with signatures checked.
    let r = act(
        &app,
        &cookie,
        "/rooms/ops/send",
        json!({ "text": "hello from the console" }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    bus.signed_op(say(
        &vega,
        "Vega",
        "ops",
        13,
        SignedOp::Send {
            body: "hi operator".into(),
        },
    ))
    .await
    .unwrap();
    let page = send(
        &app,
        get("/dashboard/api/rooms/ops/messages", Some(&cookie)),
    )
    .await
    .json();
    let msgs = page["messages"].as_array().unwrap();
    let texts: Vec<&str> = msgs.iter().map(|m| m["text"].as_str().unwrap()).collect();
    assert!(
        texts.contains(&"hello from the console") && texts.contains(&"hi operator"),
        "{page}"
    );
    assert!(
        msgs.iter()
            .all(|m| m["verified"] == true && m["signed"] == true),
        "{page}"
    );

    // A private room: only the invited join.
    act(
        &app,
        &cookie,
        "/rooms",
        json!({ "room": "backroom", "public": false }),
    )
    .await;
    let shut = bus
        .signed_op(say(&vega, "Vega", "backroom", 14, SignedOp::Join))
        .await;
    assert!(shut.is_err());
    let r = act(
        &app,
        &cookie,
        "/rooms/backroom/invite",
        json!({ "agent": "Vega" }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    bus.signed_op(say(&vega, "Vega", "backroom", 15, SignedOp::Join))
        .await
        .unwrap();
    assert_eq!(
        role_in(&overview().await, "backroom", "Vega").as_deref(),
        Some("participant")
    );
    let r = act(
        &app,
        &cookie,
        "/rooms/backroom/invite",
        json!({ "agent": "Nobody" }),
    )
    .await;
    assert_eq!(r.status, 404, "{}", r.body);

    // Direct mail both ways, verified against the sender's bound key.
    let r = act(
        &app,
        &cookie,
        "/direct",
        json!({ "to": "Vega", "text": "ping" }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let body = "pong";
    bus.send_signed(
        &common::name("Vega"),
        &common::name("operator"),
        body.into(),
        2,
        &vega.sign(&direct_payload("Vega", "operator", 2, body)),
    )
    .await
    .unwrap();
    let mail = send(&app, get("/dashboard/api/mail", Some(&cookie)))
        .await
        .json();
    let msgs = mail["messages"].as_array().unwrap();
    let pong = msgs
        .iter()
        .find(|m| m["text"] == "pong")
        .expect("the reply");
    assert_eq!(
        (pong["from"].as_str(), pong["verified"].as_bool()),
        (Some("Vega"), Some(true))
    );
    assert!(
        msgs.iter()
            .any(|m| m["text"] == "ping" && m["to"] == "Vega"),
        "{mail}"
    );
}

/// The operator obeys the room rules: in a room an agent created it is a
/// participant and cannot manage; unknown actions and rooms are refused; an
/// end-to-end encrypted room takes only role changes and leaving.
#[tokio::test]
async fn the_operator_obeys_the_room_rules() {
    let (_d, bus, app) = app_with(Some("4242"));
    traffic(&bus).await;
    let cookie = login(&app).await;
    let r = act(&app, &cookie, "/rooms/lobby/join", json!({})).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = act(
        &app,
        &cookie,
        "/rooms/lobby/kick",
        json!({ "agent": "Orion" }),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (403, json!("not_an_admin"))
    );
    let r = act(&app, &cookie, "/rooms/lobby/explode", json!({})).await;
    assert_eq!(r.status, 404);
    let r = act(&app, &cookie, "/rooms/nowhere/join", json!({})).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (404, json!("room_not_found"))
    );
    let r = act(&app, &cookie, "/rooms", json!({ "room": "lobby" })).await;
    assert_eq!(r.status, 409, "{}", r.body);

    // Vega makes the lobby end-to-end encrypted with a Commit.
    let vega = Keypair::from_seed([1; 32]);
    let body = "mls/1 commit 0 abcd";
    let commit = SignedOp::Send { body: body.into() };
    bus.signed_op(SignedRequest {
        agent: common::name("Vega"),
        room: common::room("lobby"),
        counter: 50,
        signature: vega.sign(&commit.payload("lobby", "Vega", 50)),
        op: commit,
    })
    .await
    .unwrap();
    for (action, body) in [
        ("send", json!({ "text": "hi" })),
        ("invite", json!({ "agent": "Orion" })),
    ] {
        let r = act(&app, &cookie, &format!("/rooms/lobby/{action}"), body).await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (409, json!("secure_room")),
            "{action}"
        );
    }
    let r = act(&app, &cookie, "/rooms/lobby/leave", json!({})).await;
    assert_eq!(r.status, 200, "{}", r.body);
}

/// The operator keeps its key across restarts (the same fingerprint from the
/// same data directory) and is never dropped by the idle sweep.
#[tokio::test]
async fn the_operator_is_durable() {
    use mentischat::bus::Retention;
    use std::time::Duration;
    let (dir, bus, clock) = common::bus_with_clock("operator-durable", Retention::default());
    let bus_dir = tempfile::tempdir().unwrap();
    let app = router_over(&bus, bus_dir.path(), Some("4242"));
    let cookie = login(&app).await;
    let r = act(
        &app,
        &cookie,
        "/rooms",
        json!({ "room": "ops", "public": true }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let fp = |app: Router, cookie: String| async move {
        send(&app, get("/dashboard/api/overview", Some(&cookie)))
            .await
            .json()["dashboard"]["operator"]["fingerprint"]
            .clone()
    };
    let before = fp(app.clone(), cookie.clone()).await;
    let again = router_over(&bus, bus_dir.path(), Some("4242"));
    let cookie2 = login(&again).await;
    assert_eq!(fp(again, cookie2).await, before);

    clock.advance(Retention::default().agent_ttl + Duration::from_secs(60));
    let swept = bus.sweep().await.unwrap();
    assert!(
        !swept.dropped_agents.iter().any(|a| a == "operator"),
        "{swept:?}"
    );
    drop(dir);
}

/// The sweep runs now and says what it removed.
#[tokio::test]
async fn the_operator_can_sweep_now() {
    let (_d, bus, app) = app_with(Some("4242"));
    traffic(&bus).await;
    let cookie = login(&app).await;
    let r = send(
        &app,
        post_json(
            "/dashboard/api/sweep",
            Some(&cookie),
            json!({}),
            Some(ORIGIN),
            true,
        ),
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    assert!(
        v["expiredRead"].is_i64() && v["droppedAgents"].is_array(),
        "{v}"
    );
}

/// The page talks to the daemon through one `fetch`, sends the dashboard
/// header, and has no inline event handlers.
#[test]
fn the_page_is_built_safely() {
    let html = include_str!("../src/dashboard_static/index.html");
    assert_eq!(html.matches("fetch(").count(), 1);
    assert!(html.contains("'X-MentisChat-Dashboard': '1'"));
    for handler in ["onclick=", "onerror=", "onload=", "onchange="] {
        assert!(!html.contains(handler), "{handler}");
    }
    let login = include_str!("../src/dashboard_static/login.html");
    assert!(login.contains("action=\"/dashboard/login\""));
    let setup = include_str!("../src/dashboard_static/setup.html");
    assert!(setup.contains("action=\"/dashboard/setup\""));
}

/// The PIN comes from `MENTISCHAT_DASHBOARD_PIN`, trimmed; blank is none.
#[test]
fn the_pin_comes_from_the_environment() {
    let with = |v: Option<&str>| {
        DashboardConfig::from_env(|name| {
            (name == "MENTISCHAT_DASHBOARD_PIN")
                .then(|| v.map(str::to_string))
                .flatten()
        })
        .pin
    };
    assert_eq!(with(Some(" 4242 ")), Some("4242".into()));
    assert_eq!(with(Some("  ")), None);
    assert_eq!(with(None), None);
}

/// What goes wrong around the operator is reported, never guessed at: a PIN
/// file this build cannot read counts as no PIN (the setup form), a damaged
/// key file or a bad name leaves the operator unavailable (`503`), and a name
/// another key holds fails `key_conflict`.
#[tokio::test]
async fn operator_trouble_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let files = dir.path().join("dash");
    std::fs::create_dir_all(&files).unwrap();
    std::fs::write(
        files.join(mentischat::dashboard::pin::PIN_FILE),
        "v9 nonsense\n",
    )
    .unwrap();
    std::fs::write(
        files.join(mentischat::dashboard::operator::OPERATOR_KEY_FILE),
        "not hex\n",
    )
    .unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("bus")).unwrap());
    let app = router_over(&bus, dir.path(), None);
    let r = send(&app, get("/dashboard", None)).await;
    assert_eq!(
        r.location(),
        "/dashboard/setup",
        "an unreadable PIN is no PIN"
    );
    let cookie = send(&app, setup_form("hunter22", "hunter22"))
        .await
        .cookie()
        .unwrap();
    let o = send(&app, get("/dashboard/api/overview", Some(&cookie)))
        .await
        .json();
    assert!(o["dashboard"]["operator"].is_null());
    for (path, body) in [
        ("/rooms", json!({ "room": "x" })),
        ("/rooms/x/join", json!({})),
        ("/direct", json!({ "to": "a", "text": "b" })),
    ] {
        let r = act(&app, &cookie, path, body).await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (503, json!("operator_unavailable")),
            "{path}"
        );
    }
    for path in ["/dashboard/api/mail", "/dashboard/api/rooms/x/messages"] {
        assert_eq!(
            send(&app, get(path, Some(&cookie))).await.status,
            503,
            "{path}"
        );
    }

    // A name that is not an agent name.
    let config = DashboardConfig {
        pin: Some("4242".into()),
        dir: None,
        operator: Some("not a name!".into()),
    };
    assert!(config.operator_name().is_err());
    let bad = mentischat::build_router_with(bus.clone(), config)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
    let cookie = login(&bad).await;
    let r = act(&bad, &cookie, "/rooms", json!({ "room": "x" })).await;
    assert_eq!(r.status, 503);

    // Someone else holds `operator` with its own key.
    let squatter = Keypair::from_seed([9; 32]);
    common::register(&bus, &["operator"]).await;
    bus.register_key(
        &common::name("operator"),
        squatter.public(),
        &squatter.sign(&register_key_payload("operator", &squatter.public())),
    )
    .await
    .unwrap();
    let config = DashboardConfig {
        pin: Some("4242".into()),
        dir: None,
        operator: None,
    };
    let taken = mentischat::build_router_with(bus.clone(), config)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
    let cookie = login(&taken).await;
    let r = act(&taken, &cookie, "/rooms", json!({ "room": "x" })).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (409, json!("key_conflict"))
    );
}

/// Bad requests to the room actions fail cleanly: a bad room or agent name,
/// a missing field, a message to nobody; and plain (unsigned) mail on a
/// permissive daemon shows as unsigned.
#[tokio::test]
async fn bad_room_requests_fail_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    // Permissive, so a plain message can reach the operator.
    let bus = Arc::new(Bus::new(&dir.path().join("bus")).unwrap());
    let app = router_over(&bus, dir.path(), Some("4242"));
    let cookie = login(&app).await;
    act(
        &app,
        &cookie,
        "/rooms",
        json!({ "room": "ops", "public": true }),
    )
    .await;
    let cases = [
        ("/rooms", json!({ "room": "bad room!" }), "invalid_room"),
        ("/rooms/bad%20room!/join", json!({}), "invalid_room"),
        ("/rooms/ops/send", json!({}), "invalid_body"),
        ("/rooms/ops/kick", json!({}), "invalid_body"),
        (
            "/rooms/ops/promote",
            json!({ "agent": "not a name!" }),
            "invalid_name",
        ),
        (
            "/direct",
            json!({ "to": "not a name!", "text": "x" }),
            "invalid_name",
        ),
        (
            "/direct",
            json!({ "to": "Nobody", "text": "x" }),
            "unregistered",
        ),
    ];
    for (path, body, code) in cases {
        let r = act(&app, &cookie, path, body).await;
        assert_eq!(r.json()["error"], code, "{path}: {}", r.body);
    }
    let r = send(
        &app,
        get("/dashboard/api/rooms/bad%20room!/messages", Some(&cookie)),
    )
    .await;
    assert_eq!(r.json()["error"], "invalid_room");
    let r = send(
        &app,
        get("/dashboard/api/rooms/elsewhere/messages", Some(&cookie)),
    )
    .await;
    assert!(r.status >= 400, "{}", r.body);

    common::register(&bus, &["Plain"]).await;
    bus.send(
        &common::name("Plain"),
        &common::name("operator"),
        "hi there".into(),
    )
    .await
    .unwrap();
    // A banned name with no registration is banned by name only.
    let r = act(&app, &cookie, "/rooms/ops/ban", json!({ "agent": "Ghost" })).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let mail = send(&app, get("/dashboard/api/mail", Some(&cookie)))
        .await
        .json();
    let m = &mail["messages"][0];
    assert_eq!(
        (
            m["text"].as_str(),
            m["signed"].as_bool(),
            m["verified"].as_bool()
        ),
        (Some("hi there"), Some(false), Some(false))
    );
}

/// A PIN that cannot be saved is not set: the form says so and the dashboard
/// keeps asking. (Here the data "directory" is a file.)
#[tokio::test]
async fn a_pin_that_cannot_be_saved_is_not_set() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dash"), "a file, not a directory").unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("bus")).unwrap());
    let app = router_over(&bus, dir.path(), None);
    let r = send(&app, setup_form("hunter22", "hunter22")).await;
    assert_eq!(r.location(), "/dashboard/setup?error=storage");
    assert!(r.cookie().is_none());
    let r = send(&app, get("/dashboard", None)).await;
    assert_eq!(r.location(), "/dashboard/setup");
}

/// Debug output names the PIN's source and the operator's public key, never
/// the PIN, a session or the key's seed.
#[test]
fn debug_output_keeps_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("bus")).unwrap());
    let config = DashboardConfig {
        pin: Some("sekrit-pin".into()),
        dir: Some(dir.path().join("dash")),
        operator: None,
    };
    let text = format!("{:?}", mentischat::dashboard::Dashboard::new(bus, config));
    assert!(text.contains("Env") && text.contains("operator"), "{text}");
    assert!(!text.contains("sekrit"), "{text}");
    let seed = std::fs::read_to_string(
        dir.path()
            .join("dash")
            .join(mentischat::dashboard::operator::OPERATOR_KEY_FILE),
    )
    .unwrap();
    assert!(!text.contains(seed.trim()), "{text}");
}
