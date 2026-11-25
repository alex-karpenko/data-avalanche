//! The console's app (Admin_Console §10): the files `build.rs` embedded from `ui/dist`.

/// A file of the built app: its path under `ui/dist`, and its bytes.
#[derive(Debug)]
pub struct AppFile {
    pub path: &'static str,
    pub body: &'static [u8],
}

/// The app built into this binary: empty when `ui/dist` wasn't built.
static BUILT: &[AppFile] = include!(concat!(env!("OUT_DIR"), "/app.rs"));

/// The console's app: its page (`index.html`) and the files it loads (`app/…`).
#[derive(Debug, Clone, Copy)]
pub struct App(&'static [AppFile]);

impl App {
    /// The app built into this binary.
    pub fn built() -> Self {
        Self(BUILT)
    }

    /// `files` as the app (tests; an empty one is an app not built).
    pub fn of(files: &'static [AppFile]) -> Self {
        Self(files)
    }

    /// The app's page, served for every page under `/ui/`; `None` when it isn't built.
    pub(crate) fn index(&self) -> Option<&'static [u8]> {
        self.find("index.html")
    }

    /// One of the files the page loads, `app/<name>`: its bytes and its content type.
    pub(crate) fn file(&self, name: &str) -> Option<(&'static [u8], &'static str)> {
        let path = format!("app/{name}");
        self.find(&path).map(|body| (body, content_type(&path)))
    }

    fn find(&self, path: &str) -> Option<&'static [u8]> {
        self.0.iter().find(|f| f.path == path).map(|f| f.body)
    }
}

/// The content type of a built file, by its extension.
fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, e)| e) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        Some("json") => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_built_file_s_type_follows_its_extension() {
        static FILES: &[AppFile] = &[
            AppFile {
                path: "app/index-x.css",
                body: b"",
            },
            AppFile {
                path: "app/logo.svg",
                body: b"",
            },
            AppFile {
                path: "app/data.bin",
                body: b"",
            },
        ];
        let app = App::of(FILES);
        let kind = |name| app.file(name).map(|(_, kind)| kind);
        assert_eq!(kind("index-x.css"), Some("text/css; charset=utf-8"));
        assert_eq!(kind("logo.svg"), Some("image/svg+xml"));
        assert_eq!(kind("data.bin"), Some("application/octet-stream"));
        assert_eq!(kind("other.css"), None);
        assert_eq!(app.index(), None);
    }
}
