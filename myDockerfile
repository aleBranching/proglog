FROM golang:1.23.7-alpine AS build
WORKDIR /go/src/proglog
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/proglog
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -q- -O /go/bin/grpc_health_probe \
    "https://github.com/grpc-ecosystem/grpc-health-probe/releases/tag/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64" && \
    chmod +x /go/bin/grpc_health_probe


FROM scratch
COPY --from=build /go/bin/proglog /bin/proglog
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT [ "/bin/proglog" ]
