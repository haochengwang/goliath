package main

import (
	"context"
	"flag"
	"net"
	"net/http"

	//"runtime/pprof"
	//"os"
	//"time"

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
	return s.executor.Retrieve(req), nil
}

func (s *server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	return s.executor.Search(req), nil
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
	s := grpc.NewServer()
	lis, _ := net.Listen("tcp", ":9023")
	sv := newServer()
	pb.RegisterGoliathPortalServer(s, sv)

	glog.Info("Service initialized.")
	glog.Flush()

	glog.Fatal(s.Serve(lis))
}
