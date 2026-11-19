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

/// What a route that records answers when its body is refused before it runs.
const NOTE_REFUSED: &str = "the note is too long, not a string, holds a control character, or the body isn't a JSON object";
const BODY_TOO_LARGE: &str = "the body is over 1 MiB";

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
        ("node", "a member's name, as the gossip knows it"),
        ("generation", "a config generation's number"),
        (
            "table",
            "the table, named as its config key: `schema.table` on PostgreSQL, the table alone on MySQL",
        ),
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
    if r.path == "/v1/config/diff" {
        for (name, description) in [
            ("from", "the generation compared from"),
            ("to", "the generation compared to"),
        ] {
            op = op.parameter(
                ParameterBuilder::new()
                    .name(name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::True)
                    .description(Some(description))
                    .schema(Some(
                        ObjectBuilder::new()
                            .schema_type(Type::Integer)
                            .minimum(Some(1)),
                    )),
            );
        }
    }
    if r.path == "/v1/config/generations" {
        for (name, description) in crate::generations::PARAMETERS {
            let schema = if *name == "limit" {
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .minimum(Some(1))
                    .maximum(Some(crate::generations::MAX_LIMIT))
                    .default(Some(crate::generations::DEFAULT_LIMIT.into()))
            } else {
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .minimum(Some(1))
            };
            op = op.parameter(
                ParameterBuilder::new()
                    .name(*name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::False)
                    .description(Some(*description))
                    .schema(Some(schema)),
            );
        }
    }
    if r.path == "/v1/snapshots" {
        for (name, description) in crate::snapshots::PARAMETERS {
            op = op.parameter(
                ParameterBuilder::new()
                    .name(*name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::False)
                    .description(Some(*description))
                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
            );
        }
    }
    if r.path == "/v1/history" {
        for (name, description) in crate::history::PARAMETERS {
            let schema = if *name == "step" {
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .minimum(Some(1))
            } else {
                ObjectBuilder::new().schema_type(Type::String)
            };
            op = op.parameter(
                ParameterBuilder::new()
                    .name(*name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::False)
                    .description(Some(*description))
                    .schema(Some(schema)),
            );
        }
    }
    if r.path == "/v1/stream" {
        for (name, description) in crate::stream::PARAMETERS {
            op = op.parameter(
                ParameterBuilder::new()
                    .name(*name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::False)
                    .description(Some(*description))
                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
            );
        }
    }
    if r.path == "/v1/events" {
        for (name, description) in crate::events::PARAMETERS {
            let schema = if *name == "limit" {
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .minimum(Some(1))
                    .maximum(Some(data_avalanche_state::MAX_EVENTS))
                    .default(Some(crate::events::DEFAULT_LIMIT.into()))
            } else {
                ObjectBuilder::new().schema_type(Type::String)
            };
            op = op.parameter(
                ParameterBuilder::new()
                    .name(*name)
                    .parameter_in(ParameterIn::Query)
                    .required(Required::False)
                    .description(Some(*description))
                    .schema(Some(schema)),
            );
        }
    }
    if let Some(content_type) = r.body {
        let mut body = RequestBodyBuilder::new()
            .required(Some(Required::False))
            .content(content_type, ContentBuilder::new().build());
        if r.records.is_some() {
            body = body.description(Some(format!(
                "`note`: why, kept with the action's event (optional, at most {} characters)",
                crate::MAX_NOTE
            )));
        }
        op = op.request_body(Some(body.build()));
    }
    let mut answers: Vec<(u16, String)> = r
        .answers
        .iter()
        .map(|(code, d)| (*code, (*d).to_owned()))
        .collect();
    // A route's own reason for a common answer (a `503`) beside the common one.
    for (code, common) in COMMON {
        match answers.iter_mut().find(|(c, _)| *c == code) {
            Some((_, own)) => *own = format!("{common}; or {own}"),
            None => answers.push((code, common.to_owned())),
        }
    }
    // The recording refuses a body before the route runs: its 422 beside the route's own.
    if r.records.is_some() {
        match answers.iter_mut().find(|(code, _)| *code == 422) {
            Some((_, own)) => *own = format!("{own}; or {NOTE_REFUSED}"),
            None => answers.push((422, NOTE_REFUSED.to_owned())),
        }
        answers.push((413, BODY_TOO_LARGE.to_owned()));
    }
    answers.sort_unstable();
    for (code, description) in answers {
        // The stream's messages are server-sent events; its refusals JSON as every route's.
        let content = if r.path == "/v1/stream" && code == 200 {
            "text/event-stream"
        } else {
            "application/json"
        };
        op = op.response(
            code.to_string(),
            ResponseBuilder::new()
                .description(description)
                .content(content, ContentBuilder::new().build()),
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
                let templated: Vec<&str> = path
                    .split('/')
                    .filter_map(|s| s.strip_prefix('{')?.strip_suffix('}'))
                    .collect();
                let declared: Vec<&str> = op["parameters"]
                    .as_array()
                    .map(|ps| {
                        ps.iter()
                            .filter(|p| p["in"] == "path")
                            .map(|p| p["name"].as_str().unwrap())
                            .collect()
                    })
                    .unwrap_or_default();
                assert_eq!(declared, templated, "{method} {path}");
            }
        }
        assert_eq!(operations, routes().len());
    }

    /// DA-API-18: the document is valid against the OAS 3.1 schema, vendored in `oas/` (the
    /// 2022-10-07 one, `https://spec.openapis.org/oas/3.1/schema/2022-10-07`).
    #[test]
    fn the_document_is_valid_oas_3_1() {
        const ID: &str = "https://spec.openapis.org/oas/3.1/schema/2022-10-07";
        let schema: Value =
            serde_json::from_str(include_str!("../oas/schema-2022-10-07.json")).unwrap();
        let mut schemas = boon::Schemas::new();
        let mut compiler = boon::Compiler::new();
        compiler.add_resource(ID, schema).unwrap();
        let oas = compiler.compile(ID, &mut schemas).unwrap();
        let doc = serde_json::to_value(openapi()).unwrap();
        if let Err(e) = schemas.validate(&doc, oas) {
            panic!("the document isn't valid OpenAPI 3.1: {e:#}");
        }
        let mut broken = doc;
        broken["paths"]["/v1/pipelines"]["get"]["responses"] = serde_json::json!("none");
        assert!(
            schemas.validate(&broken, oas).is_err(),
            "the schema catches a broken document"
        );
    }

    /// The recording's own answers on every route that records, and `limit`'s bounds.
    #[test]
    fn the_recorded_routes_document_the_recording_s_answers() {
        let doc: Value = serde_json::to_value(openapi()).unwrap();
        for r in routes() {
            let op = &doc["paths"][r.path][r.method.to_ascii_lowercase()];
            let answers = &op["responses"];
            if r.records.is_some() {
                assert_eq!(answers["413"]["description"], BODY_TOO_LARGE, "{}", r.path);
                let refused = answers["422"]["description"].as_str().unwrap();
                assert!(refused.ends_with(NOTE_REFUSED), "{}: {refused}", r.path);
            } else {
                assert!(answers.get("413").is_none(), "{}", r.path);
            }
        }
        let limit = doc["paths"]["/v1/events"]["get"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == "limit")
            .unwrap();
        assert_eq!(
            limit["schema"],
            serde_json::json!({ "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 })
        );
    }

    #[tokio::test]
    async fn the_document_is_served() {
        let addr = listen(api(open())).await;
        let (code, v) = json_of(addr, "GET", "/v1/openapi.json").await;
        assert_eq!(code, 200);
        assert_eq!(v, serde_json::to_value(openapi()).unwrap());
    }
}
