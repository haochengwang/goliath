package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/go-faster/city"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	pb "example.com/goliath/proto/goliath/v1"
	cpb "example.com/goliath/proto/crawler/v1"
	ppb "example.com/goliath/proto/parser/v1"

	//"example.com/goliath/internal/config"
)

type Executor struct {
	crawlerConn	*grpc.ClientConn
	parserConn	*grpc.ClientConn
	redisClient	*redis.Client
	nacosClient	config_client.IConfigClient
}

func initNacosConfigClient() config_client.IConfigClient {
	serverConfigs := []constant.ServerConfig {
		{
			IpAddr:	"10.3.0.59",
			Port:	8848,
		},
	}

	clientConfig := constant.ClientConfig {
		NamespaceId:	"2ca536a8-e24f-44d0-9bdc-5d6d2cc1cb78",
		TimeoutMs:	5000,
		LogDir:		"/tmp/nacos/log",
		CacheDir:	"/tmp/nacos/cache",
		LogLevel:	"debug",
	}

	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs":	serverConfigs,
		"clientConfig":		clientConfig,
	})

	if err != nil {
		glog.Fatal("Failed to initialize nacos config client: {err}")
	}

	return configClient
}

func NewExecutor() *Executor {
	crawlerConn, err := grpc.Dial("10.3.0.149:9023",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(10 * 1024 * 1024),
		grpc.WithInitialWindowSize(10 * 1024 * 1024))
	if err != nil {
		glog.Fatal("Failed to create conn {err}")
	}
	parserConn, err := grpc.Dial("10.3.128.4:9023",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(10 * 1024 * 1024),
		grpc.WithInitialWindowSize(10 * 1024 * 1024))
	if err != nil {
		glog.Fatal("Failed to create conn {err}")
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:		"10.3.0.103:6379",
		Password:	"2023@Ystech",
		DB:		0,
	})

	nacosClient := initNacosConfigClient()

	conf, err := nacosClient.GetConfig(vo.ConfigParam{
		DataId:	"crawler_blacklist",
		Group:	"DEFAULT_GROUP",
	})
	if err != nil {
		glog.Fatal(err)
	}
	glog.Info(conf)

	err = nacosClient.ListenConfig(vo.ConfigParam{
		DataId:	"crawler_blacklist",
		Group:	"DEFAULT_GROUP",
		OnChange:	func(namespace string, dataId string, group string, content string) {
			glog.Info("Changed")
			glog.Info(fmt.Sprintf("%s %s %s %s", namespace, dataId, group, content))
			glog.Flush()
		},
	})

	if err != nil {
		glog.Fatal(err)
	}
	return &Executor{
		crawlerConn:	crawlerConn,
		parserConn:	parserConn,
		redisClient:	redisClient,
		nacosClient:	nacosClient,
	}
}

func (e *Executor) readFromCache(url string) string {
	urlHash := city.Hash64([]byte(url))
	globalRedisKey := fmt.Sprintf("crawl:%d", urlHash)
	redisValue, err := e.redisClient.Get(context.Background(), globalRedisKey).Result()
	if err == nil {
		glog.Info("Global cache hit: ", globalRedisKey)
		return redisValue
	}
	glog.Info("Global cache miss: ", globalRedisKey)
	realtimeRedisKey := fmt.Sprintf("crawl_rt:%d", urlHash)
	redisValue, err = e.redisClient.Get(context.Background(), realtimeRedisKey).Result()
	if err == nil {
		glog.Info("Realtime cache hit: ", realtimeRedisKey)
		return redisValue
	}
	glog.Info("Realtime cache miss: ", globalRedisKey)
	return ""
}

func (e *Executor) convertParseResult(pr *ppb.ParseContentResponse) *pb.RetrieveResponse {
	if pr == nil {
		return nil
	}
	return &pb.RetrieveResponse {
		Result:	[]*pb.RetrieveResult {
			&pb.RetrieveResult {
				Url:		pr.Url,
				Content:	pr.Content,
			},
		},
	}
}

func (e *Executor) Retrieve(req *pb.RetrieveRequest) *pb.RetrieveResponse {
	glog.Info(fmt.Sprintf("Retrieve: %s", req.Url))
	defer glog.Flush()

	cacheContent := e.readFromCache(req.Url)
	if len(cacheContent) > 0 {
		parseResult := &ppb.ParseContentResponse{}
		err := proto.Unmarshal([]byte(cacheContent), parseResult)
		if err == nil {
			return e.convertParseResult(parseResult)
		}

		glog.Error("Cache content not in protobuf format, maybe corrupted! Url = ", req.Url)
	}

	c := cpb.NewCrawlServiceClient(e.crawlerConn)
	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	r, err := c.Download(ctx, &cpb.CrawlRequest{
		Url:		req.Url,
	})

	if err != nil {
		glog.Fatal(err)
	}

	if r.Content == nil || len(r.Content) == 0 {
		glog.Error("No content returned")
		glog.Error(r)
		return &pb.RetrieveResponse{}
	}
	glog.Info(r.Content)

	c2 := ppb.NewWebParserServiceClient(e.parserConn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel2()

	r2, err2 := c2.ParseContent(ctx2, &ppb.ParseRequest{
		Url:		req.Url,
		Content:	r.Content,
	})

	if err2 != nil {
		glog.Fatal(err2)
	}

	if r2.Content == nil || len(r2.Content) == 0 {
		glog.Error("No content!")
		return &pb.RetrieveResponse{}
	}
	glog.Info(r2.Content)

	return e.convertParseResult(r2)
}

func (e *Executor) Search(req * pb.SearchRequest) *pb.SearchResponse {
	return nil
}
