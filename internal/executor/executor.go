package executor

import (
	"context"
	"fmt"
	"sync"
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
	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	//"example.com/goliath/internal/utils"
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

	SUCCESS			int32 = 0
	ERR_CRAWL_FAILED	int32 = 101
	ERR_CRAWL_EMPTY		int32 = 102
	ERR_CRAWL_FEW		int32 = 103
	ERR_PARSE_FAILED	int32 = 104
	ERR_PARSE_EMPTY		int32 = 105
	ERR_PARSE_FEW		int32 = 106
	ERR_INTERNAL_ERROR	int32 = 400
)

type Executor struct {
	crawlerAddrs	map[string]string
	searchAddrs	map[string]string
	parserAddrs	map[string]string
	redisClient	*redis.Client
	nacosClient	config_client.IConfigClient

	// Kafka producer
	kafkaProducer		sarama.SyncProducer

	// Cache writer workers
	writeCacheWaitGroup	sync.WaitGroup
	writeCacheChan		chan *ppb.ParseContentResponse

	// Kafka producer workers
	kafkaWaitGroup		sync.WaitGroup
	kafkaChan		chan *cpb.CrawlResponse
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

func createGrpcConn(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(10 * 1024 * 1024),
		grpc.WithInitialWindowSize(10 * 1024 * 1024),
		grpc.WithMaxMsgSize(10 * 1024 * 1024))
}

func NewExecutor() *Executor {
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

	// Initialize kafka producer
	brokers := []string{"10.3.0.23:9092"}
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.Return.Successes = true
	kafkaProducer, err := sarama.NewSyncProducer(brokers, kafkaConfig)
	if err != nil {
		glog.Fatal(err)
	}

	writeCacheChan := make(chan *ppb.ParseContentResponse)
	kafkaChan := make(chan *cpb.CrawlResponse)
	e := &Executor{
		crawlerAddrs:	map[string]string {
			"BJ": 	"10.3.0.90:9023",
			"TOK":	"10.203.0.59:9023",
		},
		searchAddrs:	map[string]string {
			"BJ":	"10.3.0.149:9023",
		},
		parserAddrs:	map[string]string {
			"BJ":		"10.3.128.4:9023",
			"BJ-MED":	"10.3.16.19:50056",
		},
		redisClient:		redisClient,
		nacosClient:		nacosClient,
		kafkaProducer:		kafkaProducer,
		writeCacheChan:		writeCacheChan,
		kafkaChan:		kafkaChan,
	}
	writeCacheWorkerNum := 4
	e.writeCacheWaitGroup.Add(writeCacheWorkerNum)

	for range(writeCacheWorkerNum) {
		go func() {
			for r := range writeCacheChan {
				e.writeToCache(r)
			}

			e.writeCacheWaitGroup.Done()
		}()
	}

	kafkaWorkerNum := 10
	e.kafkaWaitGroup.Add(kafkaWorkerNum)

	for range(kafkaWorkerNum) {
		go func() {
			for r := range kafkaChan {
				e.kafkaProduce(r)
			}

			e.kafkaWaitGroup.Done()
		}()
	}

	return e
}

func (e *Executor) Destroy() {
	close(e.writeCacheChan)
	e.writeCacheWaitGroup.Wait()

	close(e.kafkaChan)
	e.kafkaWaitGroup.Wait()
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

func (e *Executor) writeToCache(r *ppb.ParseContentResponse) {
	url := r.Url
	urlHash := city.Hash64([]byte(url))
	realtimeRedisKey := fmt.Sprintf("crawl_rt:%d", urlHash)

	data, err := proto.Marshal(r)
	if err != nil {
		glog.Error("Failed to marshal ParseContentResponse! ", err)
		return
	}

	// Expiration set as the end of the day, which copies the logic of web search
	// TODO(wanghaocheng) Need to be optimized in the future
	_, err = e.redisClient.SetEx(context.Background(), realtimeRedisKey, data, 86400 * time.Second).Result()

	if err != nil {
		glog.Error("Error")
	} else {
		glog.Info("Successfully write to cache: ", realtimeRedisKey)
	}
}

func (e *Executor) kafkaProduce(r *cpb.CrawlResponse) {
	data, err := proto.Marshal(r)
	if err != nil {
		glog.Error("Failed to marshal CrawlResponse! ", err)
		return
	}
	partition, offset, err := e.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:		"web_search_crawl_info",
		Value:		sarama.StringEncoder(data),
	})

	if err != nil {
		glog.Error("Kafka produce message error: ", err)
	} else {
		glog.Info("Kafka produced message: partition = ", partition, ", offset = ", offset)
	}
}

func (e *Executor) convertParseResult(ctx context.Context, pr *ppb.ParseContentResponse) *pb.RetrieveResponse {
	if pr == nil {
		return nil
	}

	return &pb.RetrieveResponse {
		RequestId:	ctx.Value(ctxReqIdKey).(string),
		Result:	&pb.RetrieveResult {
			Url:		pr.Url,
			Title:		pr.Title,
			HeadTitle:	pr.HeadTitle,
			Content:	pr.Content,
			PublishTime:	pr.PublishTime,
			ImageList:	pr.ImageList,
			PositionList:	pr.PositionList,
		},
	}
}

func contextCounter(ctx context.Context, tag string, extra1 string, extra2 string, extra3 string) prom.Counter {
	return counter.WithLabelValues(
		tag,
		extra1,
		extra2,
		extra3,
		ctx.Value(ctxBizDefKey).(string))
}

func contextObserver(ctx context.Context, tag string, extra1 string, extra2 string, extra3 string) prom.Observer {
	return timer.WithLabelValues(
		tag,
		extra1,
		extra2,
		extra3,
		ctx.Value(ctxBizDefKey).(string))
}

func (e *Executor) invokeCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string, done chan interface{}) {
	startTime := time.Now()

	conn, err := createGrpcConn(e.crawlerAddrs[crawlerRegion])
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
	}

	c := cpb.NewCrawlServiceClient(conn)
	innerCtx, cancel := context.WithTimeout(context.Background(), time.Duration(req.TimeoutMs) * time.Millisecond)
	defer cancel()

	var ctype cpb.CrawlType
	// For pdf
	if req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_IMAGE {
		ctype = cpb.CrawlType_IMAGE
	} else {
		ctype = cpb.CrawlType_HTML
	}

	r, err := c.Download(innerCtx, &cpb.CrawlRequest{
		Url:		req.Url,
		RequestType:	cpb.RequestType_GET,
		UaType:		cpb.UAType_PC,
		CrawlType:	ctype,
		TimeoutMs:	req.TimeoutMs - 100,
		TaskInfo:	&cpb.TaskInfo{
			TaskName:	req.BizDef,
		},
	})

	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, err = %v, time = %f", req.Url, err, elapsedSeconds)
		done <- err
		return
	}

	if r.ErrCode != cpb.ERROR_CODE_SUCCESS {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_%s", r.ErrCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, ErrCode = %d, time = %f", req.Url, r.ErrCode, elapsedSeconds)
		done <- err
		return
	}

	if r.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_http_%d", r.StatusCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, StatusCode = %d, time = %f", req.Url, r.StatusCode, elapsedSeconds)
		done <- err
		return
	}

	if len(r.Content) == 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "empty_content", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed no content: Url = %s, time = %f", req.Url, elapsedSeconds)
		done <- err
		return
	}

	glog.Info("Crawl succeed, Url = ", req.Url, ", crawlPod = ", r.CrawlPodIp)
	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerRegion, "").Observe(elapsedSeconds)
	done <- r
}

func (e *Executor) invokeParse(ctx context.Context, req *pb.RetrieveRequest, crawled *cpb.CrawlResponse, done chan interface{}) {
	startTime := time.Now()

	var pm ppb.ParseMethod
	var parserRegion string
	if req.RetrieveType == pb.RetrieveType_PDF {
		parserRegion = "BJ-MED"
		pm = ppb.ParseMethod_PDF
	} else if req.RetrieveType == pb.RetrieveType_VIDEO_SUMMARY {
		parserRegion = "BJ-MED"
		pm = ppb.ParseMethod_BILI
	} else {
		parserRegion = "BJ"
		pm = ppb.ParseMethod_XPATH
	}

	conn, err := createGrpcConn(e.parserAddrs[parserRegion])
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
	}

	c := ppb.NewWebParserServiceClient(conn)

	innerCtx, cancel := context.WithTimeout(ctx, 100 * time.Millisecond)
	defer cancel()

	r, err := c.ParseContent(innerCtx, &ppb.ParseRequest{
		Url:		req.Url,
		ParseMethod:	pm,
		Content:	crawled.Content,
		ImgInContent:	req.DoImgUrlParse,
	})

	if err != nil {
		contextObserver(ctx, "parse", "error", parserRegion, "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed, Url = %s,  err: %v", req.Url, err)
		done <- err
		glog.Error(err)
		return
	}

	if len(r.Content) == 0 {
		contextObserver(ctx, "parse", "empty_content", parserRegion, "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed no content: Url = %s", req.Url)
		done <- err
		glog.Error(err)
		return
	}

	contextObserver(ctx, "parse", "success", parserRegion, "").Observe(time.Since(startTime).Seconds())
	done <- r
}

func (e *Executor) invokeSearch(ctx context.Context, req *pb.SearchRequest, region string, done chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			glog.Error("Panic: ", r)
		}
	}()
	startTime := time.Now()

	conn, err := createGrpcConn(e.searchAddrs[region])
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
	}

	c := cpb.NewSearchCrawlServiceClient(conn)
	innerCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs - 100) * time.Millisecond)
	defer cancel()

	r, err := c.DownloadSearchPage(innerCtx, &cpb.SearchCrawlRequest{
		Query:		req.Query,
	})

	if err != nil {
		contextObserver(ctx, "search", "error", "", "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Search failed, Query = %s, err = %v", req.Query, err)
		done <- err
		glog.Error(err)
		return
	}

	contextObserver(ctx, "search", "success", "", "").Observe(time.Since(startTime).Seconds())
	done <- r
}

func (e *Executor) convertSearchItem(ctx context.Context, t *cpb.SearchItem) *pb.SearchItem {
	return &pb.SearchItem {
		Query:		t.Query,
		Pos:		t.Pos,
		Title:		t.Title,
		Url:		t.Url,
		Summary:	t.Summary,
		ImageList:	t.ImageList,
	}
}

func (e *Executor) convertSearchResult(ctx context.Context, r *cpb.SearchCrawlResponse) *pb.SearchResponse {
	s := make([]*pb.SearchItem, len(r.SearchItemList))
	for i, item := range(r.SearchItemList) {
		s[i] = e.convertSearchItem(ctx, item)
	}
	return &pb.SearchResponse {
		RequestId:	ctx.Value(ctxReqIdKey).(string),
		RetCode:	0,
		SearchItems:	s,
		DebugString:	"",
	}
}

func (e *Executor) Retrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	defer func() {
		if r := recover(); r != nil {
			glog.Error("Panic: ", r)
		}
	}()
	startTime := time.Now()

	glog.Info(fmt.Sprintf("Bizdef: %s, Retrieve: %s, Timeout: %d", req.BizDef, req.Url, req.TimeoutMs))
	defer glog.Flush()

	if !req.BypassCache {
		cacheContent := e.readFromCache(req.Url)
		if len(cacheContent) > 0 {
			parseResult := &ppb.ParseContentResponse{}
			err := proto.Unmarshal([]byte(cacheContent), parseResult)
			if err == nil {
				elapsedSeconds := time.Since(startTime).Seconds()
				contextObserver(ctx, "retrieve", "success", "", "").Observe(elapsedSeconds)
				return e.convertParseResult(ctx, parseResult)
			}

			glog.Error("Cache content not in protobuf format, maybe corrupted! Url = ", req.Url, ", err = ", err)
		}
	}

	// TODO: parallel crawl from multiple region
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
			contextObserver(ctx, "retrieve", "crawl_failed", "", "").Observe(time.Since(startTime).Seconds())
			glog.Error(err)
			return &pb.RetrieveResponse{
				RetCode:	ERR_CRAWL_FAILED,
				DebugString:	err.Error(),
			}
		} else {
			contextObserver(ctx, "retrieve", "internal_err", "", "").Observe(time.Since(startTime).Seconds())
			return &pb.RetrieveResponse{
				RetCode:	ERR_INTERNAL_ERROR,
			}
		}
	}

	if !req.BypassCache {
		e.kafkaChan <- crawled
	}

	if crawled.Content == nil || len(crawled.Content) == 0 {
		contextObserver(ctx, "retrieve", "crawl_empty", "", "").Observe(time.Since(startTime).Seconds())
		return &pb.RetrieveResponse{
			RetCode:	ERR_CRAWL_EMPTY,
		}
	}

	if len(crawled.Content) <= 256 {
		contextObserver(ctx, "retrieve", "crawl_few", "", "").Observe(time.Since(startTime).Seconds())
		return &pb.RetrieveResponse{
			RetCode:	ERR_CRAWL_FEW,
		}
	}

	parseDone := make(chan interface{})
	defer close(parseDone)
	go e.invokeParse(ctx, req, crawled, parseDone)
	p := <-parseDone
	parsed, ok := p.(*ppb.ParseContentResponse)
	if !ok {
		err, ok := p.(error)
		if ok {
			contextObserver(ctx, "retrieve", "parse_failed", "", "").Observe(time.Since(startTime).Seconds())
			glog.Error(err)
			return &pb.RetrieveResponse{
				RetCode:	ERR_PARSE_FAILED,
				DebugString:	err.Error(),
			}
		} else {
			contextObserver(ctx, "retrieve", "internal_err", "", "").Observe(time.Since(startTime).Seconds())
			glog.Error(parsed)
			return &pb.RetrieveResponse{
				RetCode:	ERR_INTERNAL_ERROR,
			}
		}
	}

	if !req.BypassCache {
		e.writeCacheChan <- parsed
	}

	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "retrieve", "success", "", "").Observe(elapsedSeconds)
	glog.Info("Retrieve success, Url = ", req.Url, ", time = ", req.Url, elapsedSeconds)
	return e.convertParseResult(ctx, parsed)
}

func (e *Executor) Search(ctx context.Context, req * pb.SearchRequest) *pb.SearchResponse {
	glog.Info(fmt.Sprintf("Search: %s", req.Query))
	defer glog.Flush()

	region := "BJ"
	if req.ForeignHint {
		region = "TOK"
	}

	searchDone := make(chan interface{})
	defer close(searchDone)
	go e.invokeSearch(ctx, req, region, searchDone)
	r := <-searchDone
	if searched, ok := r.(*cpb.SearchCrawlResponse); ok {
		return e.convertSearchResult(ctx, searched)
	} else if err, ok := r.(error); ok {
		return &pb.SearchResponse{
			RequestId:	ctx.Value(ctxReqIdKey).(string),
			RetCode:	ERR_CRAWL_FAILED,
			DebugString:	err.Error(),
		}
	} else {
		return &pb.SearchResponse{
			RequestId:	ctx.Value(ctxReqIdKey).(string),
			RetCode:	ERR_INTERNAL_ERROR,
		}
	}
}
