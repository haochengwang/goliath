package executor

import (
	"context"
	"fmt"
	//"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/go-faster/city"
	txt "github.com/linkdotnet/golang-stringbuilder"
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

	gauge = promauto.NewGaugeVec(prom.GaugeOpts{
		Name:	"GoliathGauge",
		Help:	"Goliath Gauge",
	}, []string{"tag", "extra1", "extra2", "extra3", "extra4"})

	SUCCESS			int32 = 0
	ERR_INVALID_ARGUMENT	int32 = 1
	ERR_CRAWL_FAILED	int32 = 101
	ERR_CRAWL_EMPTY		int32 = 102
	ERR_CRAWL_FEW		int32 = 103
	ERR_PARSE_FAILED	int32 = 104
	ERR_PARSE_EMPTY		int32 = 105
	ERR_PARSE_FEW		int32 = 106
	ERR_INTERNAL_ERROR	int32 = 400
	ERR_TIMEOUT		int32 = 500
)

type Executor struct {
	crawlerAddrs	map[string]string
	searchAddrs	map[string]string
	parserAddrs	map[string]string
	redisClient	*redis.Client
	nacosClient	config_client.IConfigClient
	kafkaProducer	sarama.SyncProducer

	workerChan		chan func()
	workerWaterlevel	atomic.Int32
	waiterGroup		sync.WaitGroup
}

type CacheReaderTask struct {
	Key		string
	KeyPrefix	string
	Callback	func(string, error)
}

type CacheWriterTask struct {
	Key			string
	Value			[]byte
	ExpirationSeconds	int32
}

type CrawlAndParseResult struct {
	Res	*pb.RetrieveResponse
	CrawlContext	*pb.CrawlContext
	ParseContext	*pb.ParseContext
}

type CrawlAndParseTask struct {
	Ctx		context.Context
	Req		*pb.RetrieveRequest
	Callback	func(*CrawlAndParseResult, error)
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

	workerChan := make(chan func())
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
		workerChan:		workerChan,
	}

	workerChanNum := 8 * 1024
	e.waiterGroup.Add(8 * 1024)
	for range(workerChanNum) {
		go func() {
			for f := range workerChan {
				e.workerWaterlevel.Add(1)
				gauge.WithLabelValues("workers", "", "", "", "").Set(float64(e.workerWaterlevel.Load()))

				f()
				e.workerWaterlevel.Add(-1)
				gauge.WithLabelValues("workers", "", "", "", "").Set(float64(e.workerWaterlevel.Load()))
			}
			e.waiterGroup.Done()
		}()
	}

	return e
}

func (e *Executor) Destroy() {
	close(e.workerChan)
	e.waiterGroup.Wait()
}

func (e *Executor) readCache(task *CacheReaderTask) {
	e.workerChan <- func() {
		e.doReadCache(task)
	}
}

func (e *Executor) doReadCache(task *CacheReaderTask) {
	ret, err := e.redisClient.Get(context.Background(), task.Key).Result()
	task.Callback(ret, err)
}

func (e *Executor) writeToCache(task *CacheWriterTask) {
	e.workerChan <- func() {
		e.doWriteToCache(task)
	}
}

func (e *Executor) doWriteToCache(t *CacheWriterTask) {
	_, err := e.redisClient.Set(context.Background(), t.Key, t.Value,
		time.Duration(t.ExpirationSeconds) * time.Second).Result()

	if err != nil {
		glog.Error("Failed to write into cache, key = ", t.Key, ", Error = ", err.Error())
	}
}

func (e *Executor) kafkaProduce(r *cpb.CrawlResponse) {
	e.workerChan <- func() {
		e.doKafkaProduce(r)
	}
}

func (e *Executor) doKafkaProduce(r *cpb.CrawlResponse) {
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

func (e *Executor) convertCacheEntity(ctx context.Context, p *pb.CacheEntity) *pb.RetrieveResponse {
	if p == nil {
		return nil
	}

	return &pb.RetrieveResponse {
		Result: &pb.RetrieveResult {
			Url:	p.Url,
		},
	}
}

func convertParseResult(ctx context.Context, p *ppb.ParseContentResponse) *pb.RetrieveResponse {
	if p == nil {
		return nil
	}

	return &pb.RetrieveResponse {
		Result:	&pb.RetrieveResult {
			Url:		p.Url,
			Title:		p.Title,
			HeadTitle:	p.HeadTitle,
			Content:	p.Content,
			PublishTime:	p.PublishTime,
			ImageList:	p.ImageList,
			PositionList:	p.PositionList,
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

func contextLog(ctx context.Context, s string) {
	ctx.Value(ctxDebugStrKey).(*txt.StringBuilder).Append(s)
}

func contextLogStr(ctx context.Context) string {
	return ctx.Value(ctxDebugStrKey).(*txt.StringBuilder).ToString()
}

func (e *Executor) invokeDevRetrieve(ctx context.Context, req *pb.RetrieveRequest) {
	conn, err := createGrpcConn("10.3.32.22:9023")
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
		return
	}
	defer conn.Close()

	c := pb.NewGoliathPortalClient(conn)
	innerCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs) * time.Millisecond)
	defer cancel()

	_, err = c.Retrieve(innerCtx, req)
	if err != nil {
		glog.Error("Failed to call dev: ", err)
	}
}

func (e *Executor) invokeCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string) (*pb.CrawlContext, error) {
	startTime := time.Now()

	conn, err := createGrpcConn(e.crawlerAddrs[crawlerRegion])
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
		return &pb.CrawlContext{
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	defer conn.Close()

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

	var estimatedParseTimeMs int32 = 100
	if req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_VIDEO_SUMMARY {
		estimatedParseTimeMs = 1000
	}
	r, err := c.Download(innerCtx, &cpb.CrawlRequest{
		Url:		req.Url,
		RequestType:	cpb.RequestType_GET,
		UaType:		cpb.UAType_PC,
		CrawlType:	ctype,
		TimeoutMs:	req.TimeoutMs - estimatedParseTimeMs,
		TaskInfo:	&cpb.TaskInfo{
			TaskName:	req.BizDef,
		},
	})

	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, err = %v, time = %f", req.Url, err, elapsedSeconds)
		return &pb.CrawlContext{
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if r.ErrCode != cpb.ERROR_CODE_SUCCESS {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_%s", r.ErrCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, ErrCode = %d, time = %f", req.Url, r.ErrCode, elapsedSeconds)
		return &pb.CrawlContext{
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if r.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_http_%d", r.StatusCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Url = %s, StatusCode = %d, time = %f", req.Url, r.StatusCode, elapsedSeconds)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
			HttpCode:		r.StatusCode,
		}, err
	}

	if len(r.Content) == 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "empty_content", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed no content: Url = %s, time = %f", req.Url, elapsedSeconds)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
			HttpCode:		r.StatusCode,
			Content:		r.Content,
		}, err
	}

	//glog.Info("Crawl succeed, Url = ", req.Url, ", crawlPod = ", r.CrawlPodIp)
	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerRegion, "").Observe(elapsedSeconds)
	return &pb.CrawlContext {
		CrawlerKey:		crawlerRegion,
		CrawlTimestampMs:	startTime.UnixMilli(),
		Success:		true,
		HttpCode:		r.StatusCode,
		Content:		r.Content,
	}, nil
}

func (e *Executor) invokeParse(ctx context.Context, req *pb.RetrieveRequest, crawledContent []byte, parseTimeoutMs int32) (*ppb.ParseContentResponse, *pb.ParseContext, error) {
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
		return nil, &pb.ParseContext{
			ParserKey:		parserRegion,
			ParseTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	defer conn.Close()

	c := ppb.NewWebParserServiceClient(conn)

	innerCtx, cancel := context.WithTimeout(ctx, time.Duration(parseTimeoutMs) * time.Millisecond)
	defer cancel()

	r, err := c.ParseContent(innerCtx, &ppb.ParseRequest{
		Url:		req.Url,
		ParseMethod:	pm,
		Content:	crawledContent,
		Encoding:	"utf-8",
		ImgInContent:	req.DoImgUrlParse,
	})

	if err != nil {
		contextObserver(ctx, "parse", "error", parserRegion, "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed, err: %v", err)
		glog.Error(err)
		return nil, &pb.ParseContext{
			ParserKey:		parserRegion,
			ParseTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if len(r.Content) == 0 {
		contextObserver(ctx, "parse", "empty_content", parserRegion, "").Observe(time.Since(startTime).Seconds())
		err = fmt.Errorf("Parse failed no content")
		glog.Error(err)
		return r, &pb.ParseContext{
			ParserKey:		parserRegion,
			ParseTimestampMs:	startTime.UnixMilli(),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	contextObserver(ctx, "parse", "success", parserRegion, "").Observe(time.Since(startTime).Seconds())
	return r, &pb.ParseContext{
		ParserKey:		parserRegion,
		ParseTimestampMs:	startTime.UnixMilli(),
		Success:		true,
		Result:			convertParseResult(ctx, r).Result,
	}, err
}

func (e *Executor) invokeSearch(ctx context.Context, req *pb.SearchRequest, region string, done chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			glog.Error("Panic: ", r, "\n", string(debug.Stack()))
			glog.Flush()
		}
	}()
	startTime := time.Now()

	conn, err := createGrpcConn(e.searchAddrs[region])
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
	}
	defer conn.Close()

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

type RetrieveStatusHolder struct {
	globalCacheResult	atomic.Pointer[ppb.ParseContentResponse]
	realtimeCacheResult	atomic.Pointer[ppb.ParseContentResponse]
	asyncCacheResult	atomic.Pointer[pb.CacheEntity]
}

// Whether need to redo crawl & parse based on the content of the cache
// If not, return the response
// TODO(wanghaocheng) optimize me
func (e *Executor) needRedoCrawlAndParse(ctx context.Context, req *pb.RetrieveRequest, c *pb.CacheEntity) (bool, *pb.RetrieveResponse) {
	if c == nil {  // Need redo
		return true, nil
	}

	for _, p := range c.CachedParses {
		ms := p.ParseTimestampMs
		parseTimestamp := time.Unix(ms / 1000, ms % 1000 * 1000)
		if time.Since(parseTimestamp).Seconds() > 86400 {
			return true, nil
		} else {
			return false, &pb.RetrieveResponse {
				Result:	p.Result,
			}
		}
	}

	return true, nil
}

// Perform actual cache refresh
// TODO(wanghaocheng) optimize me
func (e *Executor) doCacheRefresh(ctx context.Context, req *pb.RetrieveRequest, ce *pb.CacheEntity, r *CrawlAndParseResult) {
	var entity *pb.CacheEntity
	if ce == nil {
		entity = &pb.CacheEntity{
			Url:		req.Url,
			CachedCrawls:	[]*pb.CrawlContext {
				r.CrawlContext,
			},
			CachedParses:	[]*pb.ParseContext {
				r.ParseContext,
			},
		}
	} else {
		entity = ce

		crawlReplaced := false
		for i, crawl := range entity.CachedCrawls {
			if crawl == nil || crawl.CrawlerKey == r.CrawlContext.CrawlerKey {
				entity.CachedCrawls[i] = r.CrawlContext
				crawlReplaced = true
				break
			}
		}

		if !crawlReplaced {
			entity.CachedCrawls = append(entity.CachedCrawls, r.CrawlContext)
		}

		parseReplaced := false
		for i, parse := range entity.CachedParses {
			if parse == nil || parse.ParserKey == r.ParseContext.ParserKey {
				entity.CachedParses[i] = r.ParseContext
				parseReplaced = true
				break
			}
		}

		if !parseReplaced {
			entity.CachedParses = append(entity.CachedParses, r.ParseContext)
		}
	}
	serializedEntity, err := proto.Marshal(entity)
	if err != nil {
		glog.Error("Failed to marshal CacheEntity proto, err = ", err)
	} else {
		glog.Info("Write to cache: ", fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))))
		e.writeToCache(&CacheWriterTask{
			Key:			fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
			Value:			serializedEntity,
			ExpirationSeconds:	0,
		})
	}
}

func (e *Executor) asyncRetrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	startTime := time.Now()
	perfExtra1, perfExtra2, perfExtra3 := "success", "", ""
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "async_retrieve", perfExtra1, perfExtra2, perfExtra3).Observe(elapsedSeconds)
		glog.Info(contextLogStr(ctx))
	}()

	if req.TimeoutMs < 100 {
		contextLog(ctx, fmt.Sprintf(" [TimeoutTooShort]"))
		perfExtra1, perfExtra2 = "error", "invalid_argument"
		return &pb.RetrieveResponse{
			RetCode:	ERR_INVALID_ARGUMENT,
			DebugString:	fmt.Sprintf("Too short timeout: %d", req.TimeoutMs),
		}
	}

	if len(req.Url) > 4 * 1024 {
		contextLog(ctx, fmt.Sprintf(" [UrlTooLong]"))
		perfExtra1, perfExtra2 = "error", "long_url"
		return &pb.RetrieveResponse{
			RetCode:	ERR_INVALID_ARGUMENT,
			DebugString:	fmt.Sprintf("Too long url: %s", req.Url),
		}
	}

	var h RetrieveStatusHolder

	asyncCacheChan := make(chan *pb.CacheEntity)
	if !req.BypassCache {
		// Process global cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("crawl:%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				elapsedSeconds := time.Since(startTime).Seconds()
				if err != nil || ret == "none" {
					contextLog(ctx, fmt.Sprintf(" [GCache miss %f]", elapsedSeconds))
					return
				}
				contextLog(ctx, fmt.Sprintf(" [GCache hit %f]", elapsedSeconds))

				parseResult := &ppb.ParseContentResponse{}
				err = proto.Unmarshal([]byte(ret), parseResult)
				if err != nil {
					glog.Error("Failed to unmarshal proto from global cache, Url = ", req.Url)
					return
				}
				h.globalCacheResult.Store(parseResult)
			},
		})

		// Process realtime cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("crawl_rt:%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				elapsedSeconds := time.Since(startTime).Seconds()
				if err != nil || ret == "none" {
					contextLog(ctx, fmt.Sprintf(" [RCache miss %f]", elapsedSeconds))
					return
				}

				contextLog(ctx, fmt.Sprintf(" [RCache hit %f]", elapsedSeconds))
				parseResult := &ppb.ParseContentResponse{}
				err = proto.Unmarshal([]byte(ret), parseResult)
				if err != nil {
					glog.Error("Failed to unmarshal proto from realtime cache, Url = ", req.Url)
					return
				}
				h.realtimeCacheResult.Store(parseResult)
			},
		})

		glog.Info("Read from cache: ", fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))))
		// Process async cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				elapsedSeconds := time.Since(startTime).Seconds()
				defer close(asyncCacheChan)
				if err != nil {  // Cache miss
					contextLog(ctx, fmt.Sprintf( "[ACache miss %f]", elapsedSeconds))
					asyncCacheChan <- nil
					return
				}
				contextLog(ctx, fmt.Sprintf( "[ACache hit %f]", elapsedSeconds))

				cacheEntity := &pb.CacheEntity{}
				err = proto.Unmarshal([]byte(ret), cacheEntity)
				if err != nil {
					glog.Error("Failed to unmarshal protobuf for async cache, Url = ", req.Url)
					asyncCacheChan <- nil
					return
				}
				h.asyncCacheResult.Store(cacheEntity)
				asyncCacheChan <- cacheEntity
			},
		})
	} else {
		go func() {
			asyncCacheChan <- nil
			close(asyncCacheChan)
		}()
	}

	var crawlParseChan chan *CrawlAndParseResult
	crawlParseChan = nil

	timeoutMs := req.TimeoutMs - int32(time.Since(startTime).Milliseconds()) - 10
	timer := time.NewTicker(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	fallback := func() *pb.RetrieveResponse {
			if p := h.asyncCacheResult.Load(); p != nil {
				perfExtra2 = "async_cache"
				contextLog(ctx, " [Ret ACache]")
				return e.convertCacheEntity(ctx, p)
			} else if p := h.realtimeCacheResult.Load(); p != nil {
				perfExtra2 = "realtime_cache"
				contextLog(ctx, " [Ret RCache]")
				return convertParseResult(ctx, p)
			} else if p := h.globalCacheResult.Load(); p != nil {
				perfExtra2 = "global_cache"
				contextLog(ctx, " [Ret GCache]")
				return convertParseResult(ctx, p)
			} else {
				perfExtra1 = "error"
				contextLog(ctx, " [Ret Failed]")
				return &pb.RetrieveResponse{
					RetCode:	ERR_INTERNAL_ERROR,
				}
			}
	}

	for {
		select {
		case ret, ok := <-asyncCacheChan:
			if !ok {
				asyncCacheChan = nil
				continue
			}

			// Evaluate cache entity
			redo, res := e.needRedoCrawlAndParse(ctx, req, ret);
			if redo {
				req.BypassCache = true
				req.TimeoutMs = 60 * 1000
				crawlParseChan = make(chan *CrawlAndParseResult)
				e.crawlAndParse(&CrawlAndParseTask{
					Ctx:	context.WithoutCancel(ctx),
					Req:	req,
					Callback:	func(res *CrawlAndParseResult, err error) {
						defer close(crawlParseChan)
						if err != nil || res == nil || res.CrawlContext == nil || res.ParseContext == nil {
							crawlParseChan <- nil
						} else {
							crawlParseChan <- res
						}
					},
				})
			} else {
				perfExtra2 = "async_cache"
				contextLog(ctx, " [Ret ACache]")
				return res
			}
		case ret, ok := <-crawlParseChan:
			if !ok {
				crawlParseChan = nil
				continue
			}

			if ret == nil {
				return fallback()
			}
			e.doCacheRefresh(ctx, req, h.asyncCacheResult.Load(), ret)

			perfExtra2 = "crawl_and_parse"
			contextLog(ctx, " [Ret CrawlAndParse]")
			return ret.Res
		case <-timer.C:
			// Fallback to cache
			return fallback()
		}
	}
}

func (e *Executor) crawlAndParse(r *CrawlAndParseTask) {
	e.workerChan <- func() {
		res, err := e.doCrawlAndParse(r.Ctx, r.Req)
		r.Callback(res, err)
	}
}

func (e *Executor) syncRetrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	res, _ := e.doCrawlAndParse(ctx, req)
	return res.Res
}

func (e *Executor) Retrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	defer func() {
		if r := recover(); r != nil {
			glog.Error("Panic: ", r, "\n", string(debug.Stack()))
			glog.Flush()
		}
	}()
	contextLog(ctx, fmt.Sprintf(" [BizDef %s]", req.BizDef))
	contextLog(ctx, fmt.Sprintf(" [URL %s]", req.Url))

	//async := rand.Intn(100) < 20
	//if async {
	//	go e.invokeDevRetrieve(ctx, req)
	//}
	return e.asyncRetrieve(ctx, req)
}

func (e *Executor) doCrawlAndParse(ctx context.Context, req *pb.RetrieveRequest) (*CrawlAndParseResult, error) {
	startTime := time.Now()
	perfExtra1, perfExtra2, perfExtra3 := "success", "", ""
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "retrieve", perfExtra1, perfExtra2, perfExtra3).Observe(elapsedSeconds)
		glog.Info(contextLogStr(ctx))
	}()

	if !req.BypassCache {
		cacheLookup := func(prefix string, cache string) *ppb.ParseContentResponse {
			cacheChan := make(chan *ppb.ParseContentResponse)
			e.readCache(&CacheReaderTask{
				Key:		fmt.Sprintf("%s%d", prefix, city.Hash64([]byte(req.Url))),
				Callback:	func(ret string, err error) {
					elapsedSeconds := time.Since(startTime).Seconds()
					if err != nil || ret == "none" {
						contextLog(ctx, fmt.Sprintf(" [%s miss %f]", cache, elapsedSeconds))
						cacheChan <- nil
						return
					}
					contextLog(ctx, fmt.Sprintf(" [%s hit %f]", cache, elapsedSeconds))

					parseResult := &ppb.ParseContentResponse{}
					err = proto.Unmarshal([]byte(ret), parseResult)
					if err != nil {
						glog.Error("Failed to unmarshal proto from global cache, Url = ", req.Url)
						cacheChan <- nil
						return
					}
					cacheChan <- parseResult
				},
			})

			return <-cacheChan
		}

		var parsed *ppb.ParseContentResponse
		parsed = cacheLookup("crawl:", "GCache")
		if parsed != nil {
			perfExtra2 = "global_cache_hit"
			return &CrawlAndParseResult{
				Res:	convertParseResult(ctx, parsed),
			}, nil
		}
		parsed = cacheLookup("crawl_rt:", "RCache")
		if parsed != nil {
			perfExtra2 = "realtime_cache_hit"
			return &CrawlAndParseResult{
				Res:	convertParseResult(ctx, parsed),
			}, nil
		}
	}

	// TODO: parallel crawl from multiple region
	region := "BJ"
	if req.ForeignHint {
		region = "TOK"
	}
	crawlContext, err := e.invokeCrawl(ctx, req, region)
	if err != nil {
		perfExtra1 = "crawl_failed"
		contextLog(ctx, fmt.Sprintf(" [CrawlFailed %s]", err.Error()))
		return &CrawlAndParseResult{
			Res:	&pb.RetrieveResponse{
				RetCode:	ERR_CRAWL_FAILED,
			},
			CrawlContext: crawlContext,
		}, nil
	}

	if !crawlContext.Success {
		perfExtra1 = "crawl_failed"
		contextLog(ctx, fmt.Sprintf(" [CrawlFailed %s]", crawlContext.ErrorMessage))
		return &CrawlAndParseResult{
			Res:	&pb.RetrieveResponse{
				RetCode:	ERR_CRAWL_FAILED,
			},
			CrawlContext: crawlContext,
		}, nil
	}

	elapsed := int32(time.Since(startTime).Milliseconds())
	if elapsed >= req.TimeoutMs {
		perfExtra1 = "timeout"
		contextLog(ctx, fmt.Sprintf(" [CrawlFailed %s]", crawlContext.ErrorMessage))
		return &CrawlAndParseResult{
			Res:	&pb.RetrieveResponse{
				RetCode:	ERR_TIMEOUT,
			},
			CrawlContext:	crawlContext,
		}, nil
	}
	parsed, parseContext, err := e.invokeParse(ctx, req, crawlContext.Content, req.TimeoutMs - elapsed)
	if err != nil {
		perfExtra1 = "parse_failed"
		contextLog(ctx, fmt.Sprintf(" [ParseFailed %s]", err.Error()))
		return &CrawlAndParseResult{
			Res:	&pb.RetrieveResponse{
				RetCode:	ERR_PARSE_FAILED,
				DebugString:	err.Error(),
			},
			CrawlContext:	crawlContext,
			ParseContext:	parseContext,
		}, nil
	}

	if !req.BypassCache {
		// Process realtime cache
		data, err := proto.Marshal(parsed)
		if err != nil {
			glog.Error("Failed to marshal ParseContentResponse! ", err)
		} else {
			e.writeToCache(&CacheWriterTask{
				Key:			fmt.Sprintf("crawl_rt:%d", city.Hash64([]byte(req.Url))),
				Value:			data,
				ExpirationSeconds:	86400,
			})
		}

		// Process async cache
		entity := &pb.CacheEntity{
			Url:		req.Url,
			CachedCrawls:	[]*pb.CrawlContext {
				crawlContext,
			},
			CachedParses:	[]*pb.ParseContext {
				parseContext,
			},
		}
		serializedEntity, err := proto.Marshal(entity)
		if err != nil {
			glog.Error("Failed to marshal CacheEntity! ", err)
		} else {
			e.writeToCache(&CacheWriterTask{
				Key:			fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
				Value:			serializedEntity,
				ExpirationSeconds:	0,
			})
		}
	}

	elapsedSeconds := time.Since(startTime).Seconds()
	contextLog(ctx, fmt.Sprintf(" [CrawlAndParseSuccess %f]", elapsedSeconds))
	perfExtra2 = "crawl_and_parse"
	return &CrawlAndParseResult{
		Res:		convertParseResult(ctx, parsed),
		CrawlContext:	crawlContext,
		ParseContext:	parseContext,
	}, nil
}

func (e *Executor) Search(ctx context.Context, req * pb.SearchRequest) *pb.SearchResponse {
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
