FROM golang:1.24

WORKDIR /ban

ENV BanDataDir=/ban/data \
    BanStratDir=/ban/strats

RUN git clone https://github.com/banbox/banstrats /ban/strats

RUN git config --global --add safe.directory "*"

# Download and install QuestDB
RUN QUESTDB_VERSION=9.3.2 && \
    QUESTDB_ASSET="questdb-${QUESTDB_VERSION}-rt-linux-x86-64.tar.gz" && \
    QUESTDB_URL="https://github.com/questdb/questdb/releases/download/${QUESTDB_VERSION}/${QUESTDB_ASSET}" && \
    mkdir -p /opt/questdb && \
    cd /tmp && \
    wget -q --show-progress "${QUESTDB_URL}" && \
    tar -xzf "${QUESTDB_ASSET}" --strip-components=1 -C /opt/questdb && \
    rm -f "${QUESTDB_ASSET}" && \
    chmod +x /opt/questdb/bin/questdb.sh

WORKDIR /ban/strats
RUN go mod tidy && \
    go build -o bot

RUN chmod +x bot && \
    ./bot init

EXPOSE 8000 8001 8812 9000

ENTRYPOINT ["/ban/strats/bot"] 
