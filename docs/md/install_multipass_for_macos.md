# Installation multipass docker for macos

> In the MacOS development environment, docker desktop consumes too many resources, so the multipass lightweight VM manager docker is recommended.

- Install multipass: https://multipass.run/ or https://github.com/canonical/multipass

```bash
sudo brew install multipass
```

- Create a Docker VM

```bash
multipass launch docker
multipass list
```

- Re-configure dockerd allow remote access

```bash
multipass exec docker -- sudo bash -c "echo '{\"hosts\":[\"unix:///var/run/docker.sock\",\"tcp://0.0.0.0:2375\"]}' > /etc/docker/daemon.json"

multipass exec docker -- sudo sed -i 's/\ -H\ fd:\/\///g' /lib/systemd/system/docker.service

multipass exec docker -- sudo systemctl daemon-reload
multipass exec docker -- sudo sudo systemctl restart docker
```

- Set Up aliases

```bash
multipass alias docker:docker docker
echo -e "alias m='multipass' \nalias d='docker'" >> /etc/bashrc && source .bashrc
```

- Test run container

```bash
docker run --rm -p 8888:80 nginx

export DOCKER_VM_IP=$(m info docker | grep -i IPv4 | awk '{print $2}')
curl -v http://${DOCKER_VM_IP}:8888
```
