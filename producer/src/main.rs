use dotenv::dotenv;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use std::{env, time::Duration};
use tokio::time::sleep;

async fn produce(brokers: &str, topic: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.server", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut i = 0;
    loop {
        let key_str = format!("{}", i);

        let _ = producer.send(
            FutureRecord::to(topic).payload("Hello").key(&key_str),
            Timeout::After(Duration::from_mins(1)),
        );

        println!("Delivery status for message {} received", i);

        i += 1;
        sleep(Duration::from_millis(100)).await;
    }
}

async fn loopfn () {
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
    let kafka_cluster_url = env::var("KAFKA_CLUSTER_URL");
    let kafka_topic = env::var("KAFKA_TOPIC");

    loopfn().await;
}
