use dotenv::dotenv;
use rand::RngExt;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::{Deserialize, Serialize};
use std::{env, time::Duration};
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug)]
struct WeatherEvent {
    station_id: String,
    temperature: f32,
    humidity: f32,
    wind_speed: f32,
    timestamp: String,
}

fn generate_weather_event(i: u64) -> WeatherEvent {
    let mut rng = rand::rng();
    
    let station_id = format!("IST-{}", i % 10);
    let temperature: f32 = rng.random_range(-50.0..60.0);
    let humidity:f32 = rng.random_range(0.0..100.0);
    let wind_speed: f32 = rng.random_range(0.00..75.0);
    let timestamp = String::from("2026-06-18T12:00:00Z");

    WeatherEvent {
        station_id,
        temperature,
        humidity,
        wind_speed,
        timestamp
    }
}

async fn produce(brokers: &str, topic: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "50000")
        .create()
        .expect("Producer creation error");

    let mut i = 0;
    loop {
        let weather_event = generate_weather_event(i);
        let serialized_event = serde_json::to_string(&weather_event).unwrap();

        let delivery_status = producer.send(
            FutureRecord::to(topic).payload(&serialized_event).key(&weather_event.station_id),
            Timeout::After(Duration::from_mins(1)),
        ).await;

        match delivery_status {
            Ok(m) => println!("Message sent! Partition: {}, Offset: {}, Timestamp: {:?}", m.partition, m.offset, m.timestamp),
            Err(e) => print!("Message not sent! Payload: {:#?}", e)
        }

        println!("Delivery status for message {} received", i);

        i += 1;
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let kafka_cluster_url = env::var("KAFKA_CLUSTER_URL").unwrap();
    let kafka_topic = env::var("KAFKA_TOPIC").unwrap();

    produce(kafka_cluster_url.as_str(), kafka_topic.as_str()).await
}
