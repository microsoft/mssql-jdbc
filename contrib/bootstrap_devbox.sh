#!/bin/bash
#
#
#       Bootstraps a Linux Devbox host for the VS Code devcontainer idempotently.
#       If your Devbox restarts, rerun this script.
#
# ---------------------------------------------------------------------------------------
#

REPO_ROOT=$(git rev-parse --show-toplevel)
NPMRC_TMPL="$REPO_ROOT/.npmrc.tmpl"
NPMRC="$REPO_ROOT/.npmrc"

DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"

# Remove Windows paths from PATH to avoid using Windows az CLI
# This allows us to mount ~/.azure from WSL.
#
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
AZ_PATH=$(which az 2>/dev/null)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi

az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

if ! [ -x "$(command -v jq)" ]; then
  echo "jq is not installed on your devbox, installing..."
  sudo apt-get update >/dev/null && sudo apt-get install -y jq >/dev/null
fi

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
fi

sudo mkdir -p /etc/docker
echo '{"max-concurrent-downloads": 32}' | sudo tee /etc/docker/daemon.json > /dev/null

echo "docker is installed, restarting..."
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
docker container ls
docker ps -q | xargs -r docker kill
az acr login --name monitoringdev --expose-token | jq -r '"echo \(.accessToken) | docker login \(.loginServer) --username 00000000-0000-0000-0000-000000000000 --password-stdin"' | bash

if ! [ -x "$(command -v npm)" ]; then
  echo "npm is not installed on your devbox, installing..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get update 2>&1 > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs
else
  echo "npm is already installed."
fi

cp $NPMRC_TMPL $NPMRC
sed -i "s/_authToken=.*/_authToken=$(az account get-access-token --resource '499b84ac-1321-427f-aa17-267ca6975798' --query accessToken --output tsv --tenant '72f988bf-86f1-41af-91ab-2d7cd011db47')/" $NPMRC
git update-index --assume-unchanged $NPMRC

sudo npm install -g @devcontainers/cli

echo "Docker: $(docker --version)"
echo "npm: $(npm version)"
echo "Dev Containers CLI: $(devcontainer --version)"