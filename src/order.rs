use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rand::Rng;

pub struct Order {
    pub id: u32,
    pub customer_id: u32,
    pub amount: f32,
    pub ts: DateTime<Utc>,
}

impl Order {
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        let random_day = rng.random_range(1..=31);
        let date = NaiveDate::from_ymd_opt(2025, 5, random_day).unwrap();
        let random_hour = rng.random_range(1..=23);
        let random_min = rng.random_range(0..=59);
        let random_sec = rng.random_range(0..=59);
        let time = NaiveTime::from_hms_opt(random_hour, random_min, random_sec).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let ts = dt.and_utc();

        Self {
            id: rng.random_range(1..10_000_000),
            customer_id: rng.random_range(1..10_000_000),
            amount: rng.random_range(1.0..10000.0),
            ts,
        }
    }
}
