mod subscription;

use std::time::Duration;

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use subscription::SubscriptionManager;

const BROKER_HOST: &str = "broker.emqx.io";
const BROKER_PORT: u16 = 1883;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prefix riêng để không đụng topic của người khác trên broker public
    let prefix = format!("rust-mqtt-sample/{}", std::process::id());

    let mut options = MqttOptions::new(format!("rust-sample-{}", std::process::id()), BROKER_HOST, BROKER_PORT);
    options.set_keep_alive(Duration::from_secs(10));

    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let manager = SubscriptionManager::new(client.clone());

    // Event loop: nhận message và dispatch đến subscription theo tên
    let dispatcher = manager.clone();
    tokio::spawn(async move {
        let mut reconnect = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("[mqtt] đã kết nối tới {BROKER_HOST}:{BROKER_PORT}");
                    // Lần đầu các subscribe đã nằm sẵn trong hàng đợi; chỉ subscribe lại khi reconnect
                    if reconnect {
                        if let Err(e) = dispatcher.resubscribe_all().await {
                            eprintln!("[mqtt] resubscribe lỗi: {e}");
                        }
                    }
                    reconnect = true;
                }
                Ok(Event::Incoming(Packet::Publish(p))) => dispatcher.dispatch(&p),
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[mqtt] lỗi kết nối: {e}, thử lại sau 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Đăng ký subscription bằng tên
    let temperature_filter = format!("{prefix}/sensors/+/temperature");
    manager
        .subscribe("temperature", &temperature_filter, QoS::AtLeastOnce, |name, topic, payload| {
            println!("[{name}] {topic} => {} °C", String::from_utf8_lossy(payload));
        })
        .await?;

    manager
        .subscribe("humidity", &format!("{prefix}/sensors/+/humidity"), QoS::AtLeastOnce, |name, topic, payload| {
            println!("[{name}] {topic} => {} %", String::from_utf8_lossy(payload));
        })
        .await?;

    manager
        .subscribe("all", &format!("{prefix}/#"), QoS::AtMostOnce, |name, topic, payload| {
            println!("[{name}] {topic} => {}", String::from_utf8_lossy(payload));
        })
        .await?;

    // Một filter chỉ thuộc về một tên
    if let Err(e) = manager.subscribe("temp-copy", &temperature_filter, QoS::AtMostOnce, |_, _, _| {}).await {
        println!("Không thể subscribe 'temp-copy': {e}");
    }

    println!("Subscriptions: {:?}", manager.names());
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish một vài message
    let messages = [
        ("sensors/room1/temperature", "25.4"),
        ("sensors/room1/humidity", "60"),
        ("sensors/room2/temperature", "27.1"),
        ("alerts/door", "open"),
    ];
    for (topic, payload) in messages {
        client
            .publish(format!("{prefix}/{topic}"), QoS::AtLeastOnce, false, payload)
            .await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Hủy subscription theo tên
    println!("\n--- Hủy subscription 'all' ---");
    manager.unsubscribe("all").await?;
    println!("Subscriptions: {:?}", manager.names());
    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .publish(format!("{prefix}/sensors/room3/temperature"), QoS::AtLeastOnce, false, "22.0")
        .await?;
    client
        .publish(format!("{prefix}/alerts/window"), QoS::AtLeastOnce, false, "closed")
        .await?; // không còn subscription nào nhận
    tokio::time::sleep(Duration::from_secs(2)).await;

    client.disconnect().await?;
    Ok(())
}
