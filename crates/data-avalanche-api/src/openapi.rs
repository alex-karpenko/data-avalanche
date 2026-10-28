//! The OpenAPI 3.1 document of the `/v1` routes, `GET /v1/openapi.json` (D-M9a.9). It is built
//! from the route table ([`crate::routes`]), as the router and its role gate are: each
//! operation's summary, its least role (`x-min-role`, and the basic or bearer credentials it
//! takes), its path parameter, its request body and its answers. The answers' bodies are JSON
//! objects, an `error` string on a refusal; their fields are not modelled.

use axum::http::StatusCode;
use axum::response::Response;
use utoipa::openapi::extensions::ExtensionsBuilder;
use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{ObjectBuilder, Type};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{
    ComponentsBuilder, ContentBuilder, InfoBuilder, OpenApi, OpenApiBuilder, Paths, Required,
    ResponseBuilder,
};

use crate::{Route, body, routes};

/// Every route's answers besides its own.
const COMMON: [(u16, &str); 3] = [
    (401, "no credentials, or ones that don't verify"),
    (403, "the caller's role is below the route's"),
    (
        503,
        "the node is starting or stopping, or its state can't be read",
    ),
];

fn operation(r: &Route) -> utoipa::openapi::path::Operation {
    let role = r.role.to_string();
    let mut op = OperationBuilder::new()
        .summary(Some(r.summary))
        .tag(r.path.split('/').nth(2).unwrap_or("v1"))
        .security(SecurityRequirement::new("basic", Vec::<String>::new()))
        .security(SecurityRequirement::new("bearer", Vec::<String>::new()))
        .extensions(Some(
            ExtensionsBuilder::new().add("x-min-role", role).build(),
        ));
    for (name, description) in [
        ("id", "the pipeline's id"),
        ("job_id", "the snapshot job's id, `<pipeline id>~<suffix>`"),
    ] {
        if r.path.contains(&format!("{{{name}}}")) {
            op = op.parameter(
                ParameterBuilder::new()
                    .name(name)
                    .parameter_in(ParameterIn::Path)
                    .required(Required::True)
                    .description(Some(description))
                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
            );
        }
    }
    if let Some(content_type) = r.body {
        op = op.request_body(Some(
            RequestBodyBuilder::new()
                .required(Some(Required::False))
                .content(content_type, ContentBuilder::new().build())
                .build(),
        ));
    }
    let mut answers = r.answers.to_vec();
    answers.extend(COMMON);
    answers.sort_unstable();
    for (code, description) in answers {
        op = op.response(
            code.to_string(),
            ResponseBuilder::new()
                .description(description)
                .content("application/json", ContentBuilder::new().build()),
        );
    }
    op.build()
}

/// The document.
pub fn openapi() -> OpenApi {
    let mut paths = Paths::new();
    for r in routes() {
        let method = match r.method {
            "GET" => HttpMethod::Get,
            "POST" => HttpMethod::Post,
            other => unreachable!("no {other} route"),
        };
        paths.add_path_operation(r.path, vec![method], operation(&r));
    }
    let components = ComponentsBuilder::new()
        .security_scheme(
            "basic",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Basic).build()),
        )
        .security_scheme(
            "bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        )
        .build();
    OpenApiBuilder::new()
        .info(
            InfoBuilder::new()
                .title("data-avalanche management API")
                .version(env!("CARGO_PKG_VERSION"))
                .description(Some(
                    "`globals.api.auth` decides which credentials a node takes: none, basic \
                     (one user, admin), or a bearer JWT whose roles are mapped (config §6.3).",
                ))
                .build(),
        )
        .paths(paths)
        .components(Some(components))
        .build()
}

pub(crate) async fn serve() -> Response {
    let doc = serde_json::to_value(openapi()).expect("the document serializes");
    body(StatusCode::OK, &doc)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::test_support::{api, json_of, open, serve as listen};

    const SNAPSHOT: &str = include_str!("../openapi.json");

    /// The document as it is checked in, `openapi.json`. `DATA_AVALANCHE_BLESS=1` writes it
    /// anew, for a change meant to change the API.
    #[test]
    fn the_document_is_the_one_checked_in() {
        let now = openapi().to_pretty_json().unwrap() + "\n";
        if std::env::var_os("DATA_AVALANCHE_BLESS").is_some() {
            std::fs::write(concat!(env!("CARGO_MANIFEST_DIR"), "/openapi.json"), &now).unwrap();
            return;
        }
        assert!(
            now == SNAPSHOT,
            "the OpenAPI document changed; if that is meant, run the test with DATA_AVALANCHE_BLESS=1 and check the diff of crates/data-avalanche-api/openapi.json"
        );
    }

    /// Every route is an operation of the document, with its role, and the document is a
    /// valid one as utoipa reads it back: OpenAPI 3.1, every security requirement a scheme of
    /// its components.
    #[test]
    fn every_route_is_in_the_document_with_its_role() {
        let doc: Value = serde_json::to_value(openapi()).unwrap();
        assert_eq!(doc["openapi"], "3.1.0");
        let back: OpenApi = serde_json::from_value(doc.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(back).unwrap(),
            doc,
            "it reads back as it is"
        );
        let schemes = doc["components"]["securitySchemes"].as_object().unwrap();
        let mut operations = 0;
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                operations += 1;
                let r = routes()
                    .into_iter()
                    .find(|r| r.path == path && r.method.eq_ignore_ascii_case(method))
                    .unwrap_or_else(|| panic!("{method} {path} is no route"));
                assert_eq!(op["x-min-role"], r.role.to_string(), "{method} {path}");
                for requirement in op["security"].as_array().unwrap() {
                    for name in requirement.as_object().unwrap().keys() {
                        assert!(schemes.contains_key(name), "{name}");
                    }
                }
                for code in ["401", "403", "503"] {
                    assert!(
                        op["responses"].get(code).is_some(),
                        "{method} {path} {code}"
                    );
                }
                for name in ["id", "job_id"] {
                    if path.contains(&format!("{{{name}}}")) {
                        assert_eq!(op["parameters"][0]["name"], name, "{method} {path}");
                    }
                }
            }
        }
        assert_eq!(operations, routes().len());
    }

    #[tokio::test]
    async fn the_document_is_served() {
        let addr = listen(api(open())).await;
        let (code, v) = json_of(addr, "GET", "/v1/openapi.json").await;
        assert_eq!(code, 200);
        assert_eq!(v, serde_json::to_value(openapi()).unwrap());
    }
}
