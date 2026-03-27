use std::net::IpAddr;
use std::path::Path;

/// Result of a GeoIP lookup containing country code, region, and coordinates.
#[derive(Debug, Clone)]
pub struct GeoIpResult {
    pub country_code: String,
    pub region: Option<String>,
    pub lat: f64,
    pub lng: f64,
}

/// Wraps a MaxMind MMDB reader for IP geolocation.
/// Returns None on construction if the database file is missing or invalid,
/// enabling graceful degradation when GeoIP is unavailable.
pub struct GeoIpReader {
    reader: maxminddb::Reader<Vec<u8>>,
}

impl GeoIpReader {
    /// Load an MMDB file from `path`. Returns `None` if the file is missing or unparseable.
    pub fn new(path: &Path) -> Option<Self> {
        match maxminddb::Reader::open_readfile(path) {
            Ok(reader) => Some(Self { reader }),
            Err(e) => {
                tracing::warn!("GeoIP database not loaded from {}: {}", path.display(), e);
                None
            }
        }
    }

    /// Look up geolocation for an IP address.
    /// Returns `None` for private/reserved IPs or if the lookup fails.
    pub fn lookup(&self, ip: IpAddr) -> Option<GeoIpResult> {
        if is_private_ip(&ip) {
            return None;
        }

        let city: maxminddb::geoip2::City = self.reader.lookup(ip).ok()?;

        let country_code = city.country?.iso_code?.to_string();
        let region = city
            .subdivisions
            .as_ref()
            .and_then(|subs| subs.first())
            .and_then(|sub| sub.iso_code)
            .map(|code| code.to_string());
        let location = city.location?;
        let lat = location.latitude?;
        let lng = location.longitude?;

        Some(GeoIpResult {
            country_code,
            region,
            lat,
            lng,
        })
    }

    /// Format lat/lng as a string suitable for `aroundLatLng` search parameter.
    pub fn format_around_lat_lng(result: &GeoIpResult) -> String {
        format!("{},{}", result.lat, result.lng)
    }
}

/// Check if an IP address is in a private/reserved range (RFC 1918, loopback, link-local, etc.)
fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()      // fc00::/7 (ULA)
                || v6.is_unicast_link_local() // fe80::/10
                || v6.to_ipv4_mapped().is_some_and(|v4| {
                    v4.is_loopback()
                        || v4.is_private()
                        || v4.is_link_local()
                        || v4.is_broadcast()
                        || v4.is_unspecified()
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::path::PathBuf;

    #[test]
    fn geoip_reader_returns_none_when_db_not_loaded() {
        let path = PathBuf::from("/nonexistent/path/GeoLite2-City.mmdb");
        let reader = GeoIpReader::new(&path);
        assert!(
            reader.is_none(),
            "Should return None for nonexistent MMDB path"
        );
    }

    /// Verify that all RFC 1918, loopback, link-local, ULA, and IPv4-mapped private addresses are classified as private, and that well-known public addresses are not.
    #[test]
    fn geoip_lookup_returns_none_for_private_ip() {
        // Even if we had a real reader, private IPs should always return None.
        // Test the is_private_ip helper directly since we may not have an MMDB file.
        let private_ips: Vec<IpAddr> = vec![
            "127.0.0.1".parse().unwrap(),
            "10.0.0.1".parse().unwrap(),
            "192.168.1.1".parse().unwrap(),
            "172.16.0.1".parse().unwrap(),
            "169.254.1.1".parse().unwrap(),
            "::1".parse().unwrap(),
            "fc00::1".parse().unwrap(),          // IPv6 ULA
            "fe80::1".parse().unwrap(),          // IPv6 link-local
            "::ffff:10.0.0.1".parse().unwrap(),  // IPv4-mapped private
            "::ffff:127.0.0.1".parse().unwrap(), // IPv4-mapped loopback
        ];

        for ip in &private_ips {
            assert!(
                is_private_ip(ip),
                "Expected {} to be classified as private",
                ip
            );
        }

        // Public IPs should NOT be private
        let public_ips: Vec<IpAddr> = vec![
            "8.8.8.8".parse().unwrap(),
            "1.1.1.1".parse().unwrap(),
            "203.0.113.1".parse().unwrap(),
        ];

        for ip in &public_ips {
            assert!(
                !is_private_ip(ip),
                "Expected {} to NOT be classified as private",
                ip
            );
        }
    }

    #[test]
    fn geoip_lookup_returns_none_when_reader_is_none() {
        // Simulate the graceful degradation path: when GeoIpReader is None,
        // callers should handle it with Option chaining.
        let reader: Option<GeoIpReader> = None;
        let ip: IpAddr = "8.8.8.8".parse().unwrap();
        let result = reader.as_ref().and_then(|r| r.lookup(ip));
        assert!(result.is_none(), "Lookup on None reader should return None");
    }

    #[test]
    fn geoip_result_contains_country_code_lat_lng() {
        // Verify GeoIpResult struct fields exist and have correct types
        let result = GeoIpResult {
            country_code: "US".to_string(),
            region: Some("CA".to_string()),
            lat: 37.751,
            lng: -97.822,
        };

        assert_eq!(result.country_code, "US");
        assert_eq!(result.region.as_deref(), Some("CA"));
        assert!((result.lat - 37.751).abs() < f64::EPSILON);
        assert!((result.lng - (-97.822)).abs() < f64::EPSILON);
    }

    /// Verify that `format_around_lat_lng` produces a comma-separated `lat,lng` string that round-trips through `f64` parsing.
    #[test]
    fn geoip_lookup_returns_coords_suitable_for_around_lat_lng() {
        // Verify that lat/lng from a GeoIpResult formats correctly for aroundLatLng param
        let result = GeoIpResult {
            country_code: "US".to_string(),
            region: None,
            lat: 37.751,
            lng: -97.822,
        };

        let formatted = GeoIpReader::format_around_lat_lng(&result);
        assert_eq!(formatted, "37.751,-97.822");

        // Verify the format can be parsed back
        let parts: Vec<&str> = formatted.split(',').collect();
        assert_eq!(parts.len(), 2);
        let _lat: f64 = parts[0].parse().expect("lat should be parseable as f64");
        let _lng: f64 = parts[1].parse().expect("lng should be parseable as f64");
    }

    /// Integration test: only runs if a real MMDB file exists at the test path.
    /// Set FLAPJACK_TEST_GEOIP_DB to point to a GeoLite2-City.mmdb file.
    #[test]
    fn geoip_lookup_with_real_db() {
        let db_path = std::env::var("FLAPJACK_TEST_GEOIP_DB").unwrap_or_default();
        if db_path.is_empty() {
            eprintln!("Skipping geoip_lookup_with_real_db: FLAPJACK_TEST_GEOIP_DB not set");
            return;
        }

        let reader = GeoIpReader::new(Path::new(&db_path))
            .expect("Should load MMDB from FLAPJACK_TEST_GEOIP_DB");

        // 8.8.8.8 is Google DNS, should resolve to US
        let ip: IpAddr = "8.8.8.8".parse().unwrap();
        let result = reader.lookup(ip);
        assert!(result.is_some(), "8.8.8.8 should have a GeoIP result");

        let result = result.unwrap();
        assert_eq!(result.country_code, "US");
        // Google DNS is roughly in the US, lat/lng should be reasonable
        assert!(
            result.lat > 20.0 && result.lat < 60.0,
            "lat should be in US range"
        );
        assert!(
            result.lng > -130.0 && result.lng < -60.0,
            "lng should be in US range"
        );

        // Private IP should return None even with a real DB
        let private_ip: IpAddr = "192.168.1.1".parse().unwrap();
        assert!(
            reader.lookup(private_ip).is_none(),
            "Private IP should return None"
        );
    }
}
