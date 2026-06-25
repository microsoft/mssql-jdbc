FROM ubuntu:24.04

# Install az CLI + python (used to vend tokens) and minimal build tools.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg lsb-release python3 python3-pip && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends azure-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY token_server.py /usr/local/bin/token_server.py
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/token_server.py

# Default port; the exporter / mirrormaker reach this via service hostname.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
