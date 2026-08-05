use super::*;

#[test]
fn rng_zero_seed_becomes_one() {
    let rng = Rng::new(0);
    assert_eq!(rng.state, 1);
}

#[test]
fn rng_nonzero_seed_kept() {
    let rng = Rng::new(42);
    assert_eq!(rng.state, 42);
}

#[test]
fn rng_deterministic() {
    let mut first = Rng::new(42);
    let mut second = Rng::new(42);
    for _ in 0..100 {
        assert_eq!(first.next_u32(), second.next_u32());
    }
}

#[test]
fn rng_next_f64_in_range() {
    let mut rng = Rng::new(123);
    for _ in 0..1000 {
        let value = rng.next_f64();
        assert!(
            (0.0..1.0).contains(&value),
            "next_f64 out of range: {value}"
        );
    }
}

#[test]
fn rng_range_within_bounds() {
    let mut rng = Rng::new(99);
    for _ in 0..500 {
        let value = rng.range(5, 10);
        assert!((5..=10).contains(&value), "range out of bounds: {value}");
    }
}

#[test]
fn rng_range_handles_equal_or_inverted_bounds() {
    let mut rng = Rng::new(1);
    assert_eq!(rng.range(7, 7), 7);
    assert_eq!(rng.range(10, 5), 10);
}

#[test]
fn rng_weighted_pick_single_weight() {
    let mut rng = Rng::new(42);
    for _ in 0..10 {
        assert_eq!(rng.weighted_pick(&[1.0]), 0);
    }
}

#[test]
fn rng_weighted_pick_extreme_weights() {
    let mut rng = Rng::new(42);
    let mut counts = [0u32; 2];
    for _ in 0..100 {
        counts[rng.weighted_pick(&[0.0, 1.0])] += 1;
    }
    assert_eq!(counts, [0, 100]);
}

#[test]
fn generate_query_id_has_32_hex_characters() {
    let mut rng = Rng::new(42);
    let id = generate_query_id(&mut rng);
    assert_eq!(id.len(), 32);
    assert!(id.chars().all(|character| character.is_ascii_hexdigit()));
}

#[test]
fn queries_for_index_selects_expected_corpus() {
    assert_eq!(queries_for_index("my_movies")[1].0, "batman");
    assert_eq!(queries_for_index("bestbuy_products")[1].0, "samsung");
    assert_eq!(queries_for_index("random_index")[1].0, "shoes");
    assert_eq!(queries_for_index("MOVIES").len(), MOVIE_QUERIES.len());
    assert_eq!(queries_for_index("MyShop").len(), PRODUCT_QUERIES.len());
}

#[test]
fn generated_identifiers_have_expected_formats() {
    let mut rng = Rng::new(42);
    let users = generate_users(&mut rng, 5);
    assert_eq!(users.len(), 5);
    assert!(users
        .iter()
        .all(|user| user.starts_with("user-") && user.len() == 13));

    let object_ids = generate_object_ids(&mut rng, 3);
    assert_eq!(object_ids.len(), 3);
    assert!(object_ids
        .iter()
        .all(|object_id| object_id.starts_with("obj-") && object_id.len() == 10));
}

#[test]
fn generated_click_positions_are_in_range() {
    let mut rng = Rng::new(42);
    for _ in 0..500 {
        assert!((1..=12).contains(&generate_click_position(&mut rng)));
    }
}

#[test]
fn generated_times_are_within_one_day() {
    let mut rng = Rng::new(42);
    let day_ms = 24 * 60 * 60 * 1000;
    for _ in 0..500 {
        assert!((0..day_ms).contains(&generate_time_of_day_ms(&mut rng)));
    }
}

#[test]
fn built_in_distributions_are_normalized() {
    let geo_sum: f64 = GEO_DISTRIBUTION
        .iter()
        .map(|(_, _, weight, _)| weight)
        .sum();
    assert!((geo_sum - 1.0).abs() < 0.05, "geo weights sum = {geo_sum}");

    let device_sum: f64 = DEVICE_TAGS.iter().map(|device| device.1).sum();
    assert!(
        (device_sum - 1.0).abs() < 0.01,
        "device weights sum = {device_sum}"
    );
}

#[test]
fn every_query_corpus_supports_no_result_generation() {
    for queries in [DEFAULT_QUERIES, MOVIE_QUERIES, PRODUCT_QUERIES] {
        assert!(queries.iter().any(|(_, _, has_results)| !has_results));
    }
}

fn options_with_search_count(days: u32, search_count: u32) -> AnalyticsSeedOptions {
    AnalyticsSeedOptions {
        search_count: Some(search_count),
        ..AnalyticsSeedOptions::for_days(days)
    }
}

#[test]
fn configured_search_count_is_spread_evenly_with_the_remainder_up_front() {
    let mut rng = Rng::new(7);
    let counts = daily_search_counts(
        &options_with_search_count(4, 10),
        chrono::Utc::now(),
        &mut rng,
    );
    assert_eq!(counts, vec![3, 3, 2, 2]);
    assert_eq!(counts.iter().sum::<u32>(), 10);
}

#[test]
fn configured_search_count_below_day_count_still_totals_exactly() {
    let mut rng = Rng::new(7);
    let counts = daily_search_counts(
        &options_with_search_count(5, 2),
        chrono::Utc::now(),
        &mut rng,
    );
    assert_eq!(counts, vec![1, 1, 0, 0, 0]);
}

#[test]
fn unconfigured_search_count_generates_one_organic_day_per_requested_day() {
    let mut rng = Rng::new(7);
    let counts = daily_search_counts(
        &AnalyticsSeedOptions::for_days(3),
        chrono::Utc::now(),
        &mut rng,
    );
    assert_eq!(counts.len(), 3);
    // 800 base * 0.6 weekend floor * 0.8 jitter floor = 384 is the lowest organic day.
    assert!(
        counts.iter().all(|count| (384..=960).contains(count)),
        "organic counts out of range: {counts:?}"
    );
}

#[test]
fn seed_options_reject_out_of_contract_values() {
    let invalid = [
        (
            AnalyticsSeedOptions::for_days(0),
            "days must be between 1 and 90",
        ),
        (
            AnalyticsSeedOptions::for_days(91),
            "days must be between 1 and 90",
        ),
        (
            options_with_search_count(1, 0),
            "searchCount must be between 1 and 100000",
        ),
        (
            options_with_search_count(1, 100_001),
            "searchCount must be between 1 and 100000",
        ),
        (
            AnalyticsSeedOptions {
                no_result_rate: Some(1.5),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "noResultRate must be between 0 and 1",
        ),
        (
            AnalyticsSeedOptions {
                no_result_rate: Some(f64::NAN),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "noResultRate must be between 0 and 1",
        ),
        (
            AnalyticsSeedOptions {
                device_distribution: Some(BTreeMap::from([("desktop".to_string(), 0.9)])),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "deviceDistribution weights must sum to 1",
        ),
        (
            AnalyticsSeedOptions {
                device_distribution: Some(BTreeMap::from([("watch".to_string(), 1.0)])),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "deviceDistribution supports desktop, mobile, and tablet",
        ),
        (
            AnalyticsSeedOptions {
                device_distribution: Some(BTreeMap::new()),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "deviceDistribution must not be empty",
        ),
        (
            AnalyticsSeedOptions {
                country_distribution: Some(BTreeMap::from([("usa".to_string(), 1.0)])),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "countryDistribution keys must be ISO alpha-2 uppercase codes",
        ),
        (
            AnalyticsSeedOptions {
                country_distribution: Some(BTreeMap::from([
                    ("US".to_string(), 1.5),
                    ("DE".to_string(), -0.5),
                ])),
                ..AnalyticsSeedOptions::for_days(1)
            },
            "countryDistribution weights must be finite and non-negative",
        ),
    ];
    for (options, expected) in invalid {
        assert_eq!(validate_seed_options(&options), Err(expected.to_string()));
    }
}

#[test]
fn seed_options_accept_the_documented_boundaries() {
    for options in [
        AnalyticsSeedOptions::for_days(1),
        AnalyticsSeedOptions::for_days(90),
        options_with_search_count(1, 100_000),
        AnalyticsSeedOptions {
            no_result_rate: Some(0.0),
            ..AnalyticsSeedOptions::for_days(1)
        },
        AnalyticsSeedOptions {
            no_result_rate: Some(1.0),
            ..AnalyticsSeedOptions::for_days(1)
        },
        AnalyticsSeedOptions {
            device_distribution: Some(BTreeMap::from([
                ("desktop".to_string(), 0.5),
                ("mobile".to_string(), 0.5),
            ])),
            country_distribution: Some(BTreeMap::from([("DE".to_string(), 1.0)])),
            ..AnalyticsSeedOptions::for_days(1)
        },
    ] {
        assert_eq!(validate_seed_options(&options), Ok(()));
    }
}

#[test]
fn configured_devices_become_platform_tags_with_their_weights() {
    let devices = resolve_devices(Some(&BTreeMap::from([
        ("desktop".to_string(), 0.25),
        ("mobile".to_string(), 0.75),
    ])));
    let tagged: Vec<_> = devices
        .iter()
        .map(|choice| (choice.tag.as_str(), choice.weight))
        .collect();
    assert_eq!(
        tagged,
        vec![("platform:desktop", 0.25), ("platform:mobile", 0.75)]
    );
}

#[test]
fn configured_country_keeps_every_reference_region_and_its_total_weight() {
    let geography = resolve_geography(Some(&BTreeMap::from([
        ("US".to_string(), 0.6),
        ("DE".to_string(), 0.4),
    ])));

    let us: Vec<_> = geography
        .iter()
        .filter(|choice| choice.country == "US")
        .collect();
    let us_reference_rows = GEO_DISTRIBUTION
        .iter()
        .filter(|entry| entry.0 == "US")
        .count();
    assert_eq!(us.len(), us_reference_rows);
    assert!(us.iter().all(|choice| choice.region.is_some()));
    // The US region breakdown is what /2/geo/US/regions renders, so every state must survive.
    assert!(
        us_reference_rows >= 10,
        "US reference rows: {us_reference_rows}"
    );

    let us_weight: f64 = us.iter().map(|choice| choice.weight).sum();
    assert!((us_weight - 0.6).abs() < 1e-9, "US weight = {us_weight}");
    // California carries 0.08 of the 0.45 built-in US weight, so 0.6 * 0.08/0.45.
    let california = us
        .iter()
        .find(|choice| choice.region.as_deref() == Some("California"))
        .expect("California row");
    assert!((california.weight - 0.6 * 0.08 / 0.45).abs() < 1e-9);

    let de: Vec<_> = geography
        .iter()
        .filter(|choice| choice.country == "DE")
        .collect();
    assert_eq!(de.len(), 1);
    assert!((de[0].weight - 0.4).abs() < 1e-9);
    assert_eq!(de[0].region, None);
    assert_eq!(de[0].ip_prefix, "46.114.5.");
}

#[test]
fn country_without_reference_data_falls_back_to_a_single_documentation_prefix() {
    let geography = resolve_geography(Some(&BTreeMap::from([("ZZ".to_string(), 1.0)])));
    assert_eq!(geography.len(), 1);
    assert_eq!(geography[0].country, "ZZ");
    assert_eq!(geography[0].region, None);
    assert_eq!(geography[0].ip_prefix, "203.0.113.");
    assert!((geography[0].weight - 1.0).abs() < 1e-9);
}

#[test]
fn resolved_geography_weights_stay_normalized() {
    let total: f64 = resolve_geography(None)
        .iter()
        .map(|choice| choice.weight)
        .sum();
    assert!((total - 1.0).abs() < 0.05, "geo weights sum = {total}");
}
