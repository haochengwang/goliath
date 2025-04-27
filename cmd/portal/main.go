package main

import (
	"context"
	"flag"
	"net"
	"net/http"

	"github.com/golang/glog"
        "github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	pb "example.com/goliath/proto/goliath/v1"
	exec "example.com/goliath/internal/executor"
)

type server struct {
	pb.GoliathPortalServer
	executor *exec.Executor
}

func newServer() *server {
	return &server{
		executor: exec.NewExecutor(),
	}
}

func (s *server) Retrieve(ctx context.Context, req *pb.RetrieveRequest) (*pb.RetrieveResponse, error) {
	return s.executor.Retrieve(ctx, req), nil
}

func (s *server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	return s.executor.Search(ctx, req), nil
}

func main() {
	flag.Parse()

	glog.Info("Initializing Goliath Portal...")
	glog.Info("  ________       .__  .__        __  .__     ")
 	glog.Info(" /  _____/  ____ |  | |__|____ _/  |_|  |__  ")
	glog.Info("/   \\  ___ /  _ \\|  | |  \\__  \\   __\\  |  \\ ")
	glog.Info("\\    \\_\\  (  <_> )  |_|  |/ __ \\|  | |   Y  \\")
 	glog.Info(" \\______  /\\____/|____/__(____  /__| |___|  /")
        glog.Info("        \\/                    \\/          \\/ ")

	go func() {
		glog.Info("Starting prometheus on port: 9464...")
		http.Handle("/metrics", promhttp.Handler())
        	http.ListenAndServe(":9464", nil)
	}()

	glog.Info("Initializing Executors...")
	s := grpc.NewServer(grpc.UnaryInterceptor(exec.GoliathInterceptor))
	lis, _ := net.Listen("tcp", ":9023")
	sv := newServer()
	defer sv.executor.Destroy()
	pb.RegisterGoliathPortalServer(s, sv)

	glog.Info("Service initialized.")
	glog.Flush()

	glog.Error(s.Serve(lis))

	sv.executor.Destroy()
}
