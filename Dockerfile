# Single build, four binaries. Runtime images stay minimal; only the
# orchestrator carries the docker CLI (it drives sandboxes over the socket).
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /bin/
COPY web /web

FROM runtime AS orchestrator-runtime
RUN apk add --no-cache docker-cli
