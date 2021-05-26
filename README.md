# kafka-subscriber

A generic kafka expose subscription framework or server, supports custom filter(e.g aviator expr), sink and record key-level sequence handling, subscriber interrupted and resumed strategy, and data is not lost.

## Developer guide

- Preconditions

```bash
git clone git@github.com/wl4g/kafka-subscriber.git
cd kafka-subscriber/tools/deploy/compose

docker-compose up -d
```

- Accessing kafka-ui: http://localhost:38080/

- Add kafka to local hosts

```bash
cat << EOF >> /etc/hosts
127.0.0.1   zookeeper01 kafka01
127.0.0.1   zookeeper02 kafka02
EOF
```

- Start with IntelliJ IDEA

TODO

## Stargazers over time

[![Stargazers over time](https://starchart.cc/wl4g/kafka-subscriber.svg)](https://starchart.cc/wl4g/kafka-subscriber)
