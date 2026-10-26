# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/otlp-metric-store-backend .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/otlp-metric-store-backend /otlp-metric-store-backend
ENV LISTEN_ADDR=0.0.0.0:4317
EXPOSE 4317
USER nonroot:nonroot
ENTRYPOINT ["/otlp-metric-store-backend"]
