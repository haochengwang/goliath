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
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	pb "example.com/goliath/proto/goliath/v1"
	cpb "example.com/goliath/proto/crawler/v1"
	ppb "example.com/goliath/proto/parser/v1"
)

var (
	timer = promauto.NewHistogramVec(prom.HistogramOpts{
		Name:	"GoliathTimer",
		Help:	"Goliath Timer",
	}, []string{"tag", "extra1", "extra2", "extra3", "extra4"})

	counter = promauto.NewCounterVec(prom.CounterOpts{
		Name:	"GoliathCounter",
		Help:	"Goliath Counter",
	}, []string{"tag", "extra1", "extra2", "extra3", "extra4"})
)

type Executor struct {
	crawlerConns	map[string]*grpc.ClientConn
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
	bjCrawlerConn, err := grpc.Dial("10.3.0.149:9023",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(10 * 1024 * 1024),
		grpc.WithInitialWindowSize(10 * 1024 * 1024))
	if err != nil {
		glog.Fatal("Failed to create grpc connection: ", err)
	}

	tokCrawlerConn, err := grpc.Dial("lb-ewshi9g6-uoztfn5ya8bvpm20.clb.na-siliconvalley.tencentclb.com:9023",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(10 * 1024 * 1024),
		grpc.WithInitialWindowSize(10 * 1024 * 1024))
	if err != nil {
		glog.Fatal("Failed to create grpc connection: ", err)
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
		crawlerConns:	map[string]*grpc.ClientConn {
			"BJ":	bjCrawlerConn,
			"TOK":	tokCrawlerConn,
		},
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
		counter.WithLabelValues("global_cache", "hit", "", "", "").Inc()
		return redisValue
	}
	counter.WithLabelValues("global_cache", "miss", "", "", "").Inc()
	glog.Info("Global cache miss: ", globalRedisKey)
	realtimeRedisKey := fmt.Sprintf("crawl_rt:%d", urlHash)
	redisValue, err = e.redisClient.Get(context.Background(), realtimeRedisKey).Result()
	if err == nil {
		glog.Info("Realtime cache hit: ", realtimeRedisKey)
		counter.WithLabelValues("realtime_cache", "hit", "", "", "").Inc()
		return redisValue
	}
	counter.WithLabelValues("realtime_cache", "miss", "", "", "").Inc()
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

func contextCounter(ctx context.Context, tag string, extra1 string, extra2 string) prom.Counter {
	return counter.WithLabelValues(
		tag,
		extra1,
		extra2,
		ctx.Value(ctxDomainKey).(string),
		ctx.Value(ctxBizDefKey).(string))
}

func contextObserver(ctx context.Context, tag string, extra1 string, extra2 string) prom.Observer {
	return timer.WithLabelValues(
		tag,
		extra1,
		extra2,
		ctx.Value(ctxDomainKey).(string),
		ctx.Value(ctxBizDefKey).(string))
}

func (e *Executor) invokeCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string, done chan interface{}) {
	startTime := time.Now()

	c := cpb.NewCrawlServiceClient(e.crawlerConns[crawlerRegion])
	innerCtx, cancel := context.WithTimeout(ctx, 15 * time.Second)
	defer cancel()

	r, err := c.Download(innerCtx, &cpb.CrawlRequest{
		Url:		req.Url,
		RequestType:	cpb.RequestType_GET,
		UaType:		cpb.UAType_PC,
		TaskInfo:	&cpb.TaskInfo{
			TaskName:	req.BizDef,
		},
	})

	if err != nil {
		contextObserver(ctx, "crawl", "error", "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Crawl failed: Url = %s, err = %v", req.Url, err)
		done <- err
		return
	}

	if r.ErrCode != cpb.ERROR_CODE_SUCCESS {
		contextObserver(ctx, "crawl", fmt.Sprintf("error_%s", r.ErrCode), "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Crawl failed: Url = %s, ErrCode = %d", req.Url, r.ErrCode)
		done <- err
		return
	}

	if r.StatusCode != 200 {
		contextObserver(ctx, "crawl", fmt.Sprintf("error_http_%d", r.StatusCode), "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Crawl failed: Url = %s, StatusCode = %d", req.Url, r.StatusCode)
		done <- err
		return
	}

	if len(r.Content) == 0 {
		contextObserver(ctx, "crawl", "empty_content", "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Crawl failed no content: Url = %s")
		done <- err
		return
	}

	contextObserver(ctx, "crawl", "success", "").Observe(time.Since(startTime).Seconds())
	done <- r
}

func (e *Executor) invokeParse(ctx context.Context, req *pb.RetrieveRequest, crawled *cpb.CrawlResponse, done chan interface{}) {
	startTime := time.Now()
	c := ppb.NewWebParserServiceClient(e.parserConn)
	innerCtx, cancel := context.WithTimeout(ctx, 1 * time.Second)
	defer cancel()

	r, err := c.ParseContent(innerCtx, &ppb.ParseRequest{
		Url:		req.Url,
		Content:	crawled.Content,
	})

	if err != nil {
		contextObserver(ctx, "parse", "error", "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed, Url = %s,  err: %v", req.Url, err)
		done <- err
		glog.Error(err)
	}

	if len(r.Content) == 0 {
		contextObserver(ctx, "parse", "empty_content", "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed no content: Url = %s")
		done <- err
		return
	}

	contextObserver(ctx, "parse", "success", "").Observe(time.Since(startTime).Seconds())
	done <- r
}

func (e *Executor) Retrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
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

	// TODO: parallel crawl from multiple site
	region := "BJ"
	if req.ForeignHint {
		region = "TOK"
	}
	crawlDone := make(chan interface{})
	defer close(crawlDone)
	go e.invokeCrawl(ctx, req, region, crawlDone)
	d := <-crawlDone

	crawled, ok := d.(*cpb.CrawlResponse)
	if !ok {
		err, ok := d.(error)
		if ok {
			glog.Error(err)
			return &pb.RetrieveResponse{
				DebugString:	err.Error(),
			}
		} else {
			return &pb.RetrieveResponse{}
		}
	}

	if crawled.Content == nil || len(crawled.Content) == 0 {
		return &pb.RetrieveResponse{}
	}

	parseDone := make(chan interface{})
	defer close(parseDone)
	go e.invokeParse(ctx, req, crawled, parseDone)
	parsed := (<-parseDone).(*ppb.ParseContentResponse)
	if parsed.Content == nil || len(parsed.Content) == 0 {
		return &pb.RetrieveResponse{}
	}

	return e.convertParseResult(parsed)
}

func (e *Executor) Search(ctx context.Context, req * pb.SearchRequest) *pb.SearchResponse {
	return nil
}
