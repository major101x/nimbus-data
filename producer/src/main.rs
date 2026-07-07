use dotenv::dotenv;
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

async fn produce(brokers: &str, topic: &str) {
    let weather_event = WeatherEvent {
        station_id: String::from("station-123"),
        temperature: 29.3,
        humidity: 60.1,
        wind_speed: 12.4,
        timestamp: String::from("2026-06-18T12:00:00Z"),
    };

    let serialized_event = serde_json::to_string(&weather_event).unwrap();

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "50000")
        .create()
        .expect("Producer creation error");

    let mut i = 0;
    loop {
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

async fn loopfn() {
    let mut i = 0;
    loop {
        println!("Delivery status for message {} sent", i);

        i += 1;
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let kafka_cluster_url = env::var("KAFKA_CLUSTER_URL").unwrap();
    let kafka_topic = env::var("KAFKA_TOPIC").unwrap();

    // loopfn().await;
    produce(kafka_cluster_url.as_str(), kafka_topic.as_str()).await
}
