# stream-connect

The streaming connect framework and server, supports cross tenant custom filter(e.g aviator expr), mapper, sinker open subscribe, data authority and record key-level sequence handling, subscribe interrupted and resumed checkpoint strategy, ensure data is not lost.

## Developer guide

- Preconditions

```bash
git clone git@github.com/wl4g/stream-connect.git
cd stream-connect/tools/deploy/compose

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

[![Stargazers over time](https://starchart.cc/wl4g/stream-connect.svg)](https://starchart.cc/wl4g/stream-connect)
