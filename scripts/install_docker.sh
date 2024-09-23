docker_compose_version="2.27.0"
curl -sSL https://get.docker.com | sh

curl -SL https://github.com/docker/compose/releases/download/v$docker_compose_version/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

systemctl enable docker
systemctl start docker
usermod -aG docker $USER
apt-get -qy upgrade
