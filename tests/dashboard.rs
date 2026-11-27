//! The operator's web dashboard through the daemon's real router: the PIN
//! login, sessions and logout, the wrong-PIN limit, the same-origin rule for
//! posts, the security headers, the overview of agents and rooms (roles,
//! fingerprints, bans), the activity feed and its previews, and the operator
//! actions, with and without a PIN.

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
    let config = DashboardConfig {
        pin: pin.map(str::to_string),
    };
    let router = mentischat::build_router_with(bus.clone(), config)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
    (dir, bus, router)
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
    assert_eq!(o["dashboard"]["pin"], true);
    assert_eq!(o["dashboard"]["previews"], true);
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

/// Without a PIN the dashboard opens, but shows no message text and refuses
/// the operator actions.
#[tokio::test]
async fn without_a_pin_there_is_no_text_and_no_actions() {
    let (_d, bus, app) = app_with(None);
    bus.watch_activity();
    traffic(&bus).await;
    let r = send(&app, get("/dashboard", None)).await;
    assert_eq!(r.status, 200);
    let r = send(&app, get("/dashboard/login", None)).await;
    assert_eq!((r.status, r.location()), (303, "/dashboard"));
    let o = send(&app, get("/dashboard/api/overview", None))
        .await
        .json();
    assert_eq!(o["dashboard"]["pin"], false);
    assert_eq!(o["dashboard"]["previews"], false);
    let a = send(&app, get("/dashboard/api/activity?after=0", None))
        .await
        .json();
    assert!(!a.to_string().contains("hello Orion"));
    for path in ["/dashboard/api/previews", "/dashboard/api/sweep"] {
        let r = send(
            &app,
            post_json(path, None, json!({ "on": true }), Some(ORIGIN), true),
        )
        .await;
        assert_eq!(r.status, 403, "{path}");
        assert_eq!(r.json()["error"], "pin_required");
    }
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
}

/// The PIN comes from `MENTISCHAT_DASHBOARD_PIN`, trimmed; blank is none.
#[test]
fn the_pin_comes_from_the_environment() {
    let with = |v: Option<&str>| {
        DashboardConfig::from_env(|name| {
            assert_eq!(name, "MENTISCHAT_DASHBOARD_PIN");
            v.map(str::to_string)
        })
        .pin
    };
    assert_eq!(with(Some(" 4242 ")), Some("4242".into()));
    assert_eq!(with(Some("  ")), None);
    assert_eq!(with(None), None);
}
