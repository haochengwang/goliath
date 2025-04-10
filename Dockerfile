FROM docker.1ms.run/golang:bookworm

RUN apt-get update && \
    apt-get install -y vim \
                       curl \
                       unzip \
                       lrzsz \
                       protobuf-compiler

ENV GOPROXY http://goproxy.cn

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

RUN mkdir /workspace

WORKDIR /workspace

COPY cmd /workspace/cmd
COPY internal /workspace/internal
COPY proto /workspace/proto
COPY go.* /workspace

COPY gen_proto.sh /workspace

RUN bash gen_proto.sh

RUN go mod tidy
RUN go mod vendor
RUN go build -o goliath_portal  -ldflags="-s -w " cmd/portal/main.go 
RUN cp goliath_portal /usr/local/bin

WORKDIR /usr/local/bin
ENTRYPOINT ["./goliath_portal"]

