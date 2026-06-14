FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /engine .

FROM alpine:3.21
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
