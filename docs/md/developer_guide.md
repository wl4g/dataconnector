# Developer Guide


## Preconditions

- [If developing on MacOS (Recommands)](install_multipass_for_macos.md)

- Clone project

```bash
git clone git@github.com/wl4g/data-connector.git
./gradlew clean build -x test -x spotbugsMain -x spotbugsTest -x checkstyleMain -x checkstyleTest -x checkstyleJmh
```

- Accessing kafka-ui: http://localhost:38080/

- Add kafka to local hosts

```bash
cat << EOF >> /etc/hosts
127.0.0.1   zookeeper01 kafka01
127.0.0.1   zookeeper02 kafka02
EOF
```

- Importing with IntelliJ IDEA/Eclipse

TODO
