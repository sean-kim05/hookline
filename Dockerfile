# Build a static hookline binary, then ship it on a minimal runtime image.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads separately from the source build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO disabled -> a fully static binary that runs on the scratch-like runtime.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/hookline ./cmd/hookline

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/hookline /usr/local/bin/hookline
EXPOSE 8080
# A failing /healthz marks the container unhealthy so orchestration can react.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
	CMD wget -qO- http://localhost:8080/healthz || exit 1
ENTRYPOINT ["hookline"]
CMD ["-addr", ":8080"]
