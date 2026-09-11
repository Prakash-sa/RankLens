FROM golang:1.22-alpine AS build
WORKDIR /src
COPY agent/go.mod agent/main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ranklens-agent .

FROM alpine:3.20
RUN addgroup -S -g 10001 ranklens \
    && adduser -S -D -H -u 10001 -G ranklens ranklens
COPY --from=build /out/ranklens-agent /usr/local/bin/ranklens-agent
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/ranklens-agent"]
