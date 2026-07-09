use dotenv::dotenv;
use metrics::{counter, describe_counter};
use metrics_exporter_prometheus::PrometheusBuilder;
use rdkafka::{
    ClientConfig, Message, consumer::{CommitMode, Consumer, StreamConsumer},
};
use std::env;
// use tokio;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &str) {
    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000");

    let consumer: StreamConsumer = config.create().expect("Consumer creation failed");

    consumer
        .subscribe(&vec![topics])
        .expect("Can't subscribe to topics");

    let builder = PrometheusBuilder::new();
    builder.with_http_listener(([0, 0, 0, 0], 9000)).install().expect("failed to install recorder/exporter");

    let events_processed_total = counter!("events_processed_total");
    let consumer_errors_total = counter!("consumer_errors_total");

    describe_counter!("events_processed_total", "The number of messages sent so far.");
    describe_counter!("consumer_errors_total", "The number of messages not sent due to errors so far.");


    loop {
        match consumer.recv().await {
            Err(e) => {
                println!("Error while receiving message: {}", e);
                consumer_errors_total.increment(1);
            },
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                    Some(Ok(p)) => p,
                };

                println!(
                    "Message received! key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    m.key(),
                    payload,
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    m.timestamp()
                );

                events_processed_total.increment(1);

                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let kafka_cluster_url = env::var("KAFKA_CLUSTER_URL").unwrap();
    let kafka_topic = env::var("KAFKA_TOPIC").unwrap();
    let kafka_consumer_group_id = env::var("KAFKA_CONSUMER_GROUP_ID").unwrap();

    consume_and_print(&kafka_cluster_url, &kafka_consumer_group_id, &kafka_topic).await;
}
