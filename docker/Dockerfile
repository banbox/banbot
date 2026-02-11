FROM golang:1.23

WORKDIR /ban

ENV BanDataDir=/ban/data \
    BanStratDir=/ban/strats

RUN git clone https://github.com/banbox/banstrats /ban/strats

RUN git config --global --add safe.directory "*"

WORKDIR /ban/strats
RUN go mod tidy && \
    go build -o ../bot

WORKDIR /ban
RUN chmod +x bot && \
    ./bot init

EXPOSE 8000 8001

ENTRYPOINT ["/ban/bot"] 
