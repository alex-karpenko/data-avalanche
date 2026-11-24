//! The console's own pages (Admin_Console §9; mockup `02-sign-in.html`), rendered by the server:
//! the sign-in, the 401 and 403 pages, and the landing page until the console's app is built
//! (M15). They are captioned in words, not status codes. Each loads `/ui/assets/console.css`, and
//! the oauth2 sign-in `/ui/assets/sign-in.js`: the Content-Security-Policy runs nothing inline.

use std::fmt::Write as _;

/// What every page shows around its card: the node that served it, and, with `auth: none`, the
/// banner that says the cluster has no authentication.
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub node: String,
    pub open: bool,
}

/// `s`, safe in HTML text and in a quoted attribute.
pub(crate) fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// A whole page: `title` in the browser's tab, `card` (HTML) in the middle, `script` loaded
/// after it when set.
fn page(frame: &Frame, title: &str, card: &str, script: Option<&str>) -> String {
    let banner = if frame.open {
        r#"<div class="warnband" role="alert">⚠ This cluster has no authentication: anyone who can reach it is an admin. Set <code>globals.api.auth</code>.</div>"#
    } else {
        ""
    };
    let script = script.map_or(String::new(), |src| {
        format!(r#"<script src="{src}"></script>"#)
    });
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} · data-avalanche</title>
<link rel="stylesheet" href="/ui/assets/console.css">
</head>
<body>
{banner}<main><div class="card">
<div class="brand">data-avalanche<small>console</small></div>
{card}
<div class="foot">{node} · {version}</div>
</div></main>
{script}</body>
</html>
"#,
        title = escape(title),
        node = escape(&frame.node),
        version = env!("CARGO_PKG_VERSION"),
    )
}

/// The `basic` sign-in: a form, not the browser's popup, so signing out works. `error` and
/// `info` are shown above the button.
pub(crate) fn basic_sign_in(
    frame: &Frame,
    next: &str,
    user: &str,
    error: Option<&str>,
    info: Option<&str>,
) -> String {
    let mut card = format!(
        r#"<div class="muted">Sign in to the cluster</div>
<form method="post" action="/ui/sign-in">
<input type="hidden" name="next" value="{next}">
<label for="user">User</label>
<input type="text" id="user" name="user" value="{user}" autocomplete="username" required autofocus>
<label for="password">Password</label>
<input type="password" id="password" name="password" autocomplete="current-password" required>
"#,
        next = escape(next),
        user = escape(user),
    );
    if let Some(e) = error {
        let _ = writeln!(
            card,
            r#"<div class="msg err" role="alert">{}</div>"#,
            escape(e)
        );
    }
    if let Some(i) = info {
        let _ = writeln!(card, r#"<div class="msg info">{}</div>"#, escape(i));
    }
    card.push_str("<button class=\"btn\" type=\"submit\">Sign in</button>\n</form>");
    page(frame, "Sign in", &card, None)
}

/// 401 with a session (`basic`, `oauth2`): it ended, why, and a way back to `next`.
pub(crate) fn ended(frame: &Frame, why: &str, next: &str) -> String {
    let card = format!(
        r#"<div class="title">Your session has ended</div>
<p>{why}.</p>
<p class="muted">Sign in again to go back to <code>{next}</code>.</p>
<a class="btn" href="/ui/sign-in?next={encoded}">Sign in again</a>"#,
        why = escape(&capitalized(why)),
        next = escape(next),
        encoded = escape(&encode(next)),
    );
    page(frame, "Session ended", &card, None)
}

/// 401 with `jwt`: there is nothing to sign in to; the proxy in front sends the token.
pub(crate) fn no_token(frame: &Frame, why: &str) -> String {
    let card = format!(
        r#"<div class="title">No valid token</div>
<p>This console expects a bearer token on every request, from the proxy in front of it: {why}.</p>
<p class="muted">Reload once the proxy sends a token.</p>"#,
        why = escape(why),
    );
    page(frame, "No valid token", &card, None)
}

/// 403: who the caller is, and why that isn't enough; with a session, a way out.
pub(crate) fn forbidden(frame: &Frame, name: &str, why: &str, session: bool) -> String {
    let mut card = format!(
        r#"<div class="title">Forbidden</div>
<p>You are <b>{name}</b>, and {why}.</p>
<p class="muted">Ask an admin to map one of your roles through <code>globals.api.role_mapping</code>.</p>"#,
        name = escape(name),
        why = escape(why),
    );
    if session {
        card.push_str(SIGN_OUT);
    }
    page(frame, "Forbidden", &card, None)
}

/// 503: nobody can be verified right now.
pub(crate) fn unavailable(frame: &Frame, why: &str) -> String {
    let card = format!(
        r#"<div class="title">Signing in isn't possible right now</div>
<p>{why}.</p>
<p class="muted">Try again in a moment.</p>"#,
        why = escape(&capitalized(why)),
    );
    page(frame, "Unavailable", &card, None)
}

/// 429: this address failed to sign in too often; it waits `seconds`.
pub(crate) fn waiting(frame: &Frame, next: &str, seconds: u64) -> String {
    let card = format!(
        r#"<div class="title">Too many failed sign-ins</div>
<p>Too many wrong passwords came from your address. Try again in {seconds} s.</p>
<a class="btn sec" href="/ui/sign-in?next={encoded}">Back to the sign-in</a>"#,
        encoded = escape(&encode(next)),
    );
    page(frame, "Wait", &card, None)
}

/// 403: a form posted from another site.
pub(crate) fn cross_site(frame: &Frame) -> String {
    let card = r#"<div class="title">Forbidden</div>
<p>This form was sent from another site. Sign in from the console's own page.</p>
<a class="btn sec" href="/ui/sign-in">Go to the sign-in</a>"#;
    page(frame, "Forbidden", card, None)
}

/// The sign-out form: a session's way out.
const SIGN_OUT: &str = r#"
<form method="post" action="/ui/sign-out"><button class="btn sec" type="submit">Sign out</button></form>"#;

/// The landing page, until the console's app is built (M15): who is signed in, and, with a
/// session, a way out.
pub(crate) fn signed_in(frame: &Frame, name: &str, role: &str, session: bool) -> String {
    let mut card = format!(
        r#"<div class="title">Signed in</div>
<p>You are <b>{name}</b>, signed in as <b>{role}</b>.</p>"#,
        name = escape(name),
        role = escape(role),
    );
    if session {
        card.push_str(SIGN_OUT);
    }
    page(frame, "Console", &card, None)
}

/// `s` with its first letter in capitals.
fn capitalized(s: &str) -> String {
    let mut c = s.chars();
    c.next().map_or(String::new(), |first| {
        first.to_uppercase().chain(c).collect()
    })
}

/// `s` as a query parameter's value.
pub(crate) fn encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
