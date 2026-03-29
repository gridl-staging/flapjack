use std::sync::Arc;

use crate::dto::SearchRequest;

use flapjack::types::FieldValue;

/// Extracts a `(lat, lng)` pair from a `FieldValue` geo object.
pub(super) fn extract_single_geoloc(value: &FieldValue) -> Option<(f64, f64)> {
    match value {
        FieldValue::Object(map) => {
            let lat = match map.get("lat")? {
                FieldValue::Float(f) => *f,
                FieldValue::Integer(i) => *i as f64,
                _ => return None,
            };
            let lng = match map.get("lng")? {
                FieldValue::Float(f) => *f,
                FieldValue::Integer(i) => *i as f64,
                _ => return None,
            };
            Some((lat, lng))
        }
        _ => None,
    }
}

pub(super) fn extract_all_geolocs(geoloc: Option<&FieldValue>) -> Vec<(f64, f64)> {
    match geoloc {
        None => vec![],
        Some(FieldValue::Object(_)) => extract_single_geoloc(geoloc.unwrap()).into_iter().collect(),
        Some(FieldValue::Array(arr)) => arr.iter().filter_map(extract_single_geoloc).collect(),
        _ => vec![],
    }
}

/// Selects the best geo point from a document's `_geoloc` array: the point closest
/// to the `aroundLatLng` center that also passes the geo filter, or the first passing point.
pub(super) fn best_geoloc_for_filter(
    points: &[(f64, f64)],
    geo_params: &flapjack::query::geo::GeoParams,
) -> Option<(f64, f64)> {
    if points.is_empty() {
        return None;
    }
    if let Some(ref center) = geo_params.around {
        points
            .iter()
            .filter(|(lat, lng)| geo_params.filter_point(*lat, *lng))
            .min_by(|(lat_a, lng_a), (lat_b, lng_b)| {
                let da = flapjack::query::geo::haversine(center.lat, center.lng, *lat_a, *lng_a);
                let db = flapjack::query::geo::haversine(center.lat, center.lng, *lat_b, *lng_b);
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
    } else {
        points
            .iter()
            .find(|(lat, lng)| geo_params.filter_point(*lat, *lng))
            .copied()
    }
}

/// Applies query-rule-injected geo parameters (`aroundLatLng`, `aroundRadius`) on top
/// of the request's geo params, clearing bounding box/polygon filters when a rule center is set.
pub(super) fn apply_rule_geo_overrides(
    mut geo_params: flapjack::query::geo::GeoParams,
    rule_around_lat_lng: Option<&str>,
    rule_around_radius: Option<&serde_json::Value>,
) -> flapjack::query::geo::GeoParams {
    if let Some(parsed_center) =
        rule_around_lat_lng.and_then(flapjack::query::geo::parse_around_lat_lng)
    {
        // aroundLatLng from rules must take precedence over request geo anchors.
        geo_params.around = Some(parsed_center);
        geo_params.bounding_boxes.clear();
        geo_params.polygons.clear();
    }

    if let Some(parsed_radius) =
        rule_around_radius.and_then(flapjack::query::geo::parse_around_radius)
    {
        if geo_params.around.is_some() {
            geo_params.around_radius = Some(parsed_radius);
        }
    }

    geo_params
}

/// Resolve country and region from the client IP using GeoIP lookup.
/// Returns `(country_code, region)` if the IP resolves, `(None, None)` otherwise.
/// Gracefully degrades: no reader, no IP, private IP, or parse failure all return `(None, None)`.
fn borrow_geoip_reader(
    geoip_reader: &Option<Arc<crate::geoip::GeoIpReader>>,
) -> Option<&crate::geoip::GeoIpReader> {
    geoip_reader.as_deref()
}

pub(super) fn resolve_country_region_from_ip(
    user_ip: &Option<String>,
    geoip_reader: &Option<Arc<crate::geoip::GeoIpReader>>,
) -> (Option<String>, Option<String>) {
    let Some(ip) = parse_client_ip_for_geo(user_ip.as_deref()) else {
        return (None, None);
    };
    let Some(reader) = borrow_geoip_reader(geoip_reader) else {
        return (None, None);
    };
    match reader.lookup(ip) {
        Some(result) => (Some(result.country_code), result.region),
        None => (None, None),
    }
}

/// Resolve `aroundLatLngViaIP` by looking up the client's IP in the GeoIP database.
pub(super) fn should_resolve_around_lat_lng_via_ip(req: &SearchRequest) -> bool {
    req.around_lat_lng_via_ip == Some(true)
        && req.around_lat_lng.is_none()
        && req.inside_bounding_box.is_none()
        && req.inside_polygon.is_none()
}

pub(super) fn parse_client_ip_for_geo(user_ip: Option<&str>) -> Option<std::net::IpAddr> {
    user_ip.and_then(|raw_ip| raw_ip.parse::<std::net::IpAddr>().ok())
}

/// Resolves the client IP to use for GeoIP-based `aroundLatLng` auto-detection.
pub(super) fn resolve_geoip_lookup_ip(req: &SearchRequest) -> Option<std::net::IpAddr> {
    if !should_resolve_around_lat_lng_via_ip(req) {
        return None;
    }

    let Some(ip_str) = req.user_ip.as_deref() else {
        tracing::debug!("aroundLatLngViaIP=true but no client IP available");
        return None;
    };

    let Some(ip) = parse_client_ip_for_geo(Some(ip_str)) else {
        tracing::warn!("aroundLatLngViaIP: failed to parse client IP '{}'", ip_str);
        return None;
    };

    Some(ip)
}

pub(super) fn resolve_geoip_reader(
    geoip_reader: &Option<Arc<crate::geoip::GeoIpReader>>,
) -> Option<&crate::geoip::GeoIpReader> {
    let Some(reader) = borrow_geoip_reader(geoip_reader) else {
        tracing::debug!("aroundLatLngViaIP=true but no GeoIP reader available");
        return None;
    };
    Some(reader)
}

/// Sets `aroundLatLng` on the request by looking up the client IP via GeoIP.
pub(super) fn resolve_around_lat_lng_via_ip(
    req: &mut SearchRequest,
    geoip_reader: &Option<Arc<crate::geoip::GeoIpReader>>,
) {
    let Some(ip) = resolve_geoip_lookup_ip(req) else {
        return;
    };
    let Some(reader) = resolve_geoip_reader(geoip_reader) else {
        return;
    };

    if let Some(result) = reader.lookup(ip) {
        let coords = crate::geoip::GeoIpReader::format_around_lat_lng(&result);
        tracing::debug!("aroundLatLngViaIP resolved {} -> {}", ip, coords);
        req.around_lat_lng = Some(coords);
    }
}
