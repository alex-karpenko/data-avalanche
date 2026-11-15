//! What a node tells the others about itself beyond its phase (Admin_Console §10, D-M13b.2–3):
//! where its API is, and a short hash of its own config file, so another node's console page can
//! reach it and say whether its file still matches the generation it runs.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_config::Api;
use data_avalanche_state::config_hash;

use crate::Advertiser;

/// How often a node reads its file again for its hash.
pub const FILE_CHECK: Duration = Duration::from_secs(15);

/// The URL the other nodes reach this node's API at (D-M13b.2): `api.advertise_url`, else the
/// scheme its TLS sets, the listener's host (`advertised`, the gossip's address, when it listens
/// on all addresses) and the listener's port.
pub fn api_url(api: &Api, advertised: IpAddr) -> String {
    if let Some(url) = &api.advertise_url {
        return url.trim_end_matches('/').to_owned();
    }
    let scheme = if api.tls.is_some() { "https" } else { "http" };
    let host = api
        .address
        .host
        .trim_start_matches('[')
        .trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(ip) if ip.is_unspecified() => {
            format!(
                "{scheme}://{}",
                SocketAddr::new(advertised, api.address.port)
            )
        }
        Ok(ip) => format!("{scheme}://{}", SocketAddr::new(ip, api.address.port)),
        Err(_) => format!("{scheme}://{host}:{}", api.address.port),
    }
}

/// The first 16 hex digits of the text's `config_hash`: enough to tell two files apart, short
/// enough for every identity in a gossip packet.
pub fn file_hash(text: &str) -> String {
    let hash = config_hash(text);
    let hex = hash.strip_prefix("blake3:").unwrap_or(&hash);
    hex[..16].to_owned()
}

/// The hash of the file at `path`; empty when it can't be read.
async fn hash_of(path: &PathBuf) -> String {
    match tokio::fs::read_to_string(path).await {
        Ok(text) => file_hash(&text),
        Err(e) => {
            tracing::debug!(path = %path.display(), "the config file for its hash: {e}");
            String::new()
        }
    }
}

/// Reads the node's file every [`FILE_CHECK`] and gossips its hash when it changed from `last`
/// (a ConfigMap updated under a running node). Runs until the gossip is gone.
pub async fn watch_file(path: PathBuf, mut last: String, advertiser: Advertiser) {
    loop {
        tokio::time::sleep(FILE_CHECK).await;
        let now = hash_of(&path).await;
        if now != last {
            tracing::info!(path = %path.display(), hash = %now, "the config file changed");
            if !advertiser.set_file_hash(now.clone()) {
                return;
            }
            last = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_config::{
        ApiAuth, FingerprintKey, HostPort, Origin, Pem, Secret, TlsIdentity,
    };

    use super::*;

    fn api(address: &str, tls: bool, advertise_url: Option<&str>) -> Api {
        let pem = || Pem {
            origin: Origin::Inline,
            text: Secret::new("x", &FingerprintKey::random()),
        };
        Api {
            address: address.parse::<HostPort>().unwrap(),
            advertise_url: advertise_url.map(str::to_owned),
            auth: ApiAuth::None,
            credentials: None,
            jwt: None,
            oauth2: None,
            role_mapping: Default::default(),
            tls: tls.then(|| TlsIdentity {
                cert: pem(),
                key: pem(),
            }),
        }
    }

    const GOSSIP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 1, 12));

    #[test]
    fn the_api_url_is_derived_from_the_listener_and_the_gossip() {
        assert_eq!(
            api_url(&api("0.0.0.0:8080", false, None), GOSSIP),
            "http://10.0.1.12:8080"
        );
        assert_eq!(
            api_url(&api("[::]:8443", true, None), GOSSIP),
            "https://10.0.1.12:8443"
        );
        assert_eq!(
            api_url(&api("127.0.0.1:9000", false, None), GOSSIP),
            "http://127.0.0.1:9000"
        );
        let v6 = IpAddr::V6("fd00::12".parse().unwrap());
        assert_eq!(
            api_url(&api("0.0.0.0:8080", false, None), v6),
            "http://[fd00::12]:8080"
        );
        assert_eq!(
            api_url(&api("cdc.local:8080", true, None), GOSSIP),
            "https://cdc.local:8080"
        );
    }

    #[test]
    fn advertise_url_wins_without_its_trailing_slash() {
        let a = api("0.0.0.0:8080", true, Some("https://cdc-0.cdc.ns.svc:8080/"));
        assert_eq!(api_url(&a, GOSSIP), "https://cdc-0.cdc.ns.svc:8080");
    }

    #[test]
    fn a_file_hash_is_the_config_hash_s_first_16_digits() {
        let h = file_hash("config_version: 1\n");
        assert_eq!(h.len(), 16);
        assert!(config_hash("config_version: 1\n").starts_with(&format!("blake3:{h}")));
        assert_ne!(h, file_hash("config_version: 1\n# changed\n"));
    }
}
