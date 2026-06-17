# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/lbsync ./cmd/lbsync

FROM gcr.io/distroless/static-debian12:latest
COPY --from=build /out/lbsync /lbsync
# 8623: metrics/health HTTP, 3320: Olric, 3322: memberlist gossip
EXPOSE 8623 3320 3322
ENTRYPOINT ["/lbsync"]
