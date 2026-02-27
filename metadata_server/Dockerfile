FROM golang:1.23-alpine AS builder

# Create a workspace for the app
WORKDIR /app

# Download necessary Go modules
COPY cmd/mdata/go.mod ./
RUN go mod download

# Copy over the source files
COPY cmd/mdata/. ./

# Build
RUN go build -o /main

ENTRYPOINT ["/main"]


FROM alpine:latest AS runner
RUN apk --no-cache add curl
WORKDIR /

# Copy from builder the final binary
COPY --from=builder /main /main

EXPOSE 8080

ENTRYPOINT ["/main"]
