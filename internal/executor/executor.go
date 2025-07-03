package executor

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/go-faster/city"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	// "github.com/nacos-group/nacos-sdk-go/v2/vo"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	redis "github.com/redis/go-redis/v9"
	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"example.com/goliath/internal/utils"
	"example.com/goliath/internal/config"
	pb "example.com/goliath/proto/goliath/v1"
	cpb "example.com/goliath/proto/crawler/v1"
	ppb "example.com/goliath/proto/parser/v1"
)

var (
	// Flags
	IsDev = flag.Bool("dev", false, "Is in dev env")

	// Prometheus metrics
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

	cosCrawlBucket = "web-crawl-1319140468"

	// Ret codes
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
	conf		*config.JsonConfig
	crawlerAddrs	map[string]string
	searchAddrs	map[string]string
	parserAddrs	map[string]string
	redisClient	*redis.Client
	nacosClient	config_client.IConfigClient
	kafkaProducer	sarama.SyncProducer

	workerChan		chan *WorkerTask
	workerWaterlevel	map[string]*atomic.Int32
	waiterGroup		sync.WaitGroup
}

type WorkerTask struct {
	Category	string
	Func		func()
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

type KafkaProduceTask struct {
	Topic		string
	Message		[]byte
}

type CrawlAndParseResult struct {
	Res	*pb.RetrieveResponse
	CrawlContext	*pb.CrawlContext
	ParseContext	*pb.ParseContext
}

type CrawlAndParseTask struct {
	Ctx		context.Context
	Req		*pb.RetrieveRequest
	CacheEntity	*pb.CacheEntity
	BypassCache	bool		// If not present, use the timeout in req
	TimeoutMs	int32		// If not present, use the timeout in req
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
		grpc.WithInitialConnWindowSize(100 * 1024 * 1024),
		grpc.WithInitialWindowSize(100 * 1024 * 1024),
		grpc.WithMaxMsgSize(100 * 1024 * 1024))
}

func NewExecutor() *Executor {
	conf, err := config.LoadConfig("./conf/CONFIG.json")
	if err != nil {
		glog.Fatal(err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:		"10.3.0.103:6379",
		Password:	"2023@Ystech",
		DB:		0,
	})

	/*
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
	*/

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

	workerChan := make(chan *WorkerTask)
	e := &Executor{
		conf:		conf,
		crawlerAddrs:	map[string]string {
			"BJ": 		"10.3.0.90:9023",
			"BJNEW":	"10.3.32.116:9023",
			"BJ:RENDER":	"http://10.3.8.41:30001/download/",
			"TOK":		"10.203.0.59:9023",
			// Via nginx
			"SV-ALI":	"47.254.88.250:9023",
			"SV-ALI-NGINX":	"47.254.88.250:13001",
		},
		searchAddrs:	map[string]string {
			"BJ":	"10.3.0.149:9023",
		},
		parserAddrs:	map[string]string {
			"BJ":		"10.3.128.4:9023",
			"BJ-MED":	"10.3.16.19:50056",
		},
		redisClient:		redisClient,
		//nacosClient:		nacosClient,
		kafkaProducer:		kafkaProducer,
		workerChan:		workerChan,
		workerWaterlevel:	map[string]*atomic.Int32 {
			"crawl_and_parse":	&atomic.Int32{},
			"read_cache":		&atomic.Int32{},
			"write_cache":		&atomic.Int32{},
			"retrieve_cycle":	&atomic.Int32{},
			"redirect":		&atomic.Int32{},
			"kafka_produce":	&atomic.Int32{},
			"upload_to_cos":	&atomic.Int32{},
			"finalize":		&atomic.Int32{},
		},
	}

	workerChanNum := 8 * 1024
	e.waiterGroup.Add(workerChanNum)
	for range(workerChanNum) {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					glog.Error("Panic: ", r, "\n", string(debug.Stack()))
					glog.Flush()
				}
			}()
			for task := range workerChan {
				v, _ := e.workerWaterlevel[task.Category]
				v.Add(1)
				gauge.WithLabelValues("workers", task.Category, "", "", "").Set(float64(v.Load()))

				task.Func()

				v.Add(-1)
				gauge.WithLabelValues("workers", task.Category, "", "", "").Set(float64(v.Load()))
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

func (e *Executor) asyncWorkerCall(category string, f func()) {
	e.workerChan <- &WorkerTask{
		Category:	category,
		Func:		f,
	}
}

func (e *Executor) readCache(task *CacheReaderTask) {
	e.asyncWorkerCall("read_cache", func() {
		e.doReadCache(task)
	})
}

func (e *Executor) doReadCache(task *CacheReaderTask) {
	ret, err := e.redisClient.Get(context.Background(), task.Key).Result()
	task.Callback(ret, err)
}

func (e *Executor) writeToCache(task *CacheWriterTask) {
	e.asyncWorkerCall("write_cache", func() {
		e.doWriteToCache(task)
	})
}

func (e *Executor) doWriteToCache(t *CacheWriterTask) {
	_, err := e.redisClient.Set(context.Background(), t.Key, t.Value,
		time.Duration(t.ExpirationSeconds) * time.Second).Result()

	if err != nil {
		glog.Error("Failed to write into cache, key = ", t.Key, ", Error = ", err.Error())
	}
}

func (e *Executor) kafkaProduce(r *KafkaProduceTask) {
	e.asyncWorkerCall("kafka_produce", func() {
		e.doKafkaProduce(r)
	})
}

func (e *Executor) doKafkaProduce(t *KafkaProduceTask) {
	partition, offset, err := e.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:		t.Topic,
		Value:		sarama.StringEncoder(t.Message),
	})

	if err != nil {
		glog.Error("Kafka produce message error, partition = ", partition, ", offset = ", offset, ", err = ", err)
	}
}

func (e *Executor) convertCacheEntity(ctx context.Context, req *pb.RetrieveRequest, p *pb.CacheEntity) *pb.RetrieveResponse {
	if p == nil {
		return nil
	}

	for _, parse := range p.CachedParses {
		if parse.Success {
			r := &pb.RetrieveResponse {
				Result: parse.Result,
			}

			// For history crawl entities
			if r.Result.SourceContentType == pb.ContentType_TYPE_UNKNOWN {
				if req.RetrieveType == pb.RetrieveType_PDF {
					r.Result.SourceContentType = pb.ContentType_TYPE_PDF
				} else {
					r.Result.SourceContentType = pb.ContentType_TYPE_WEB_PAGE
				}
			}

			return r
		}
	}
	return nil
}

func convertImageCrawlContext(ctx context.Context, req *pb.RetrieveRequest, c *pb.CrawlContext) *CrawlAndParseResult {
	if c == nil {
		return nil
	}

	r := &CrawlAndParseResult {
		Res:	&pb.RetrieveResponse {
			Result: &pb.RetrieveResult {
				Url:		req.Url,
				Content:	c.Content,
			},
		},
		CrawlContext:	c,
		ParseContext:	&pb.ParseContext {
			ParserKey:		"NA",
			ParseTimestampMs:	time.Now().UnixMilli(),
			Success:		true,
			Result:	&pb.RetrieveResult {
				Url:		req.Url,
				Content:	c.Content,
			},
			ContentLen:		int64(len(c.Content)),
		},
	}

	return r
}

func convertParseResult(ctx context.Context, p *ppb.ParseContentResponse) *pb.RetrieveResponse {
	if p == nil {
		return nil
	}

	r := &pb.RetrieveResponse {
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

	return r
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
	ctx.Value(ctxDebugStrKey).(*utils.StringBuilder).WriteString(s)
}

func contextLogStr(ctx context.Context) string {
	return ctx.Value(ctxDebugStrKey).(*utils.StringBuilder).String()
}

func (e *Executor) invokeDevRetrieve(ctx context.Context, req *pb.RetrieveRequest) {
	conn, err := createGrpcConn("10.3.32.22:9023")
	if err != nil {
		glog.Error("Failed to create grpc connection: ", err)
		return
	}
	defer conn.Close()

	c := pb.NewGoliathPortalClient(conn)
	innerCtx, cancel := context.WithTimeout(context.Background(), time.Duration(int32(req.TimeoutMs) + 100) * time.Millisecond)
	defer cancel()

	_, err = c.Retrieve(innerCtx, req)
	if err != nil {
		glog.Error("Failed to call dev: ", err)
	}
}

func isPDF(req *pb.RetrieveRequest, contentType string) bool {
	if len(contentType) == 0 {
		return req.RetrieveType == pb.RetrieveType_PDF
	}
	return strings.Contains(contentType, "pdf")
}

func (e *Executor) invokeParse(ctx context.Context, req *pb.RetrieveRequest, crawlContext *pb.CrawlContext, parseTimeoutMs int32) (*ppb.ParseContentResponse, *pb.ParseContext, error) {
	startTime := time.Now()

	var pm ppb.ParseMethod
	var parserRegion string

	crawledContent := crawlContext.Content

	if isPDF(req, crawlContext.ContentType) {
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
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "parse", "error", parserRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Parse failed, err: %v", err)
		return nil, &pb.ParseContext{
			ParserKey:		parserRegion,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if len(r.Content) == 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "parse", "empty_content", parserRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Parse failed no content")
		return r, &pb.ParseContext{
			ParserKey:		parserRegion,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "parse", "success", parserRegion, "").Observe(elapsedSeconds)

	result := convertParseResult(ctx, r).Result
	// If PublishTime is not parsed, use last modified
	if len(result.PublishTime) == 0 {
		result.PublishTime = crawlContext.LastModified
	}
	return r, &pb.ParseContext{
		ParserKey:		parserRegion,
		ParseTimestampMs:	startTime.UnixMilli(),
		ParseTimecostMs:	int64(elapsedSeconds * 1000),
		Success:		true,
		Result:			result,
		ContentLen:		int64(len(r.Content)),
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
	crawlAndParseResult	atomic.Pointer[CrawlAndParseResult]

	// Final response
	retrieveResponse	atomic.Pointer[pb.RetrieveResponse]

	minReturnTimeMs		atomic.Int32
}

// Whether need to redo crawl & parse based on the content of the cache
// If not, return the response
// TODO(wanghaocheng) optimize me
func (e *Executor) needRedoCrawlAndParse(ctx context.Context, req *pb.RetrieveRequest, c *pb.CacheEntity) bool {
	if c == nil {  // Need redo
		return true
	}

	for _, p := range c.CachedParses {
		if !p.Success || p.Result == nil || len(p.Result.Content) == 0 {
			continue
		}
		ms := p.ParseTimestampMs
		parseTimestamp := time.Unix(ms / 1000, ms % 1000 * 1000)
		// TODO(wanghaocheng) optimize me
		if req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_IMAGE {
			return false
		}
		if time.Since(parseTimestamp).Seconds() > 86400 {
			return true
		} else {
			return false
		}
	}

	return true
}

func (e *Executor) copyCrawlContextAndUploadToCos(ctx context.Context, req *pb.RetrieveRequest, c *pb.CrawlContext) *pb.CrawlContext {
	now := time.Now()
	cosnPath := ""
	if c.Content != nil && c.Success && len(c.Content) > 0 {
		content := c.Content
		cosnPath = fmt.Sprintf("crawl/%d-%d-%d/url-%d", now.Year(), now.Month(), now.Day(), city.Hash64([]byte(req.Url)))
		e.asyncWorkerCall("upload_to_cos", func() {
			uploadToCos(cosCrawlBucket, cosnPath, content)
		})
	}
	return &pb.CrawlContext {
		CrawlerKey:		c.CrawlerKey,
		CrawlTimestampMs:	c.CrawlTimestampMs,
		Success:		c.Success,
		ErrorMessage:		c.ErrorMessage,
		HttpCode:		c.HttpCode,
		ContentType:		c.ContentType,
		ContentEncoding:	c.ContentEncoding,
		// emit content
		CrawlTimecostMs:	c.CrawlTimecostMs,
		ContentLen:		c.ContentLen,
		ContentCosnPath:	cosnPath,
	}
}

// Perform actual cache refresh
// TODO(wanghaocheng) optimize me
func (e *Executor) doCacheRefresh(ctx context.Context, req *pb.RetrieveRequest, ce *pb.CacheEntity, r *CrawlAndParseResult) {
	var entity *pb.CacheEntity
	if ce == nil {
		entity = &pb.CacheEntity{
			Url:		req.Url,
		}

		if r.CrawlContext != nil && r.CrawlContext.Success {
			entity.CachedCrawls = []*pb.CrawlContext { e.copyCrawlContextAndUploadToCos(ctx, req, r.CrawlContext) }
		}
		if r.ParseContext != nil && r.ParseContext.Success {
			entity.CachedParses = []*pb.ParseContext { r.ParseContext }
		}
	} else {
		entity = &pb.CacheEntity{
			Url:		req.Url,
			CachedCrawls:	ce.CachedCrawls,
			CachedParses:	ce.CachedParses,
		}

		if r.CrawlContext != nil && r.CrawlContext.Success {
			crawlReplaced := false
			for i, crawl := range entity.CachedCrawls {
				if crawl == nil || crawl.CrawlerKey == r.CrawlContext.CrawlerKey {
					// Compare crawl timestamp to ensure the crawl is based on the cached one
					if crawl.CrawlTimestampMs != r.CrawlContext.CrawlTimestampMs {
						entity.CachedCrawls[i] = e.copyCrawlContextAndUploadToCos(ctx, req, r.CrawlContext)
					}
					crawlReplaced = true
					break
				}
			}

			if !crawlReplaced {
				entity.CachedCrawls = append(entity.CachedCrawls, e.copyCrawlContextAndUploadToCos(ctx, req, r.CrawlContext))
			}
		}

		if r.ParseContext != nil && r.ParseContext.Success {
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
	}
	serializedEntity, err := proto.Marshal(entity)
	if err != nil {
		glog.Error("Failed to marshal CacheEntity proto, err = ", err)
	} else {
		e.writeToCache(&CacheWriterTask{
			Key:			fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
			Value:			serializedEntity,
			ExpirationSeconds:	0,
		})
	}
}

func (e *Executor) doCacheTidy(ctx context.Context, req *pb.RetrieveRequest, ce *pb.CacheEntity) {
	modified := false
	for i, crawl := range ce.CachedCrawls {
		if crawl.Content != nil && len(crawl.Content) > 0 {
			ce.CachedCrawls[i] = e.copyCrawlContextAndUploadToCos(ctx, req, crawl)
			modified = true
		}
	}

	if !modified {
		return
	}

	serializedEntity, err := proto.Marshal(ce)
	if err != nil {
		glog.Error("Failed to marshal CacheEntity proto, err = ", err)
	} else {
		e.writeToCache(&CacheWriterTask{
			Key:			fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
			Value:			serializedEntity,
			ExpirationSeconds:	0,
		})
	}
}

func (e *Executor) asyncRetrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	startTime := time.Now()
	resultSource := "Failed"
	perfExtra1, perfExtra2, perfExtra3 := "success", "", ""

	// Channel monitoring the finish of the request
	reqFinishChan := make(chan int, 1)
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextLog(ctx, fmt.Sprintf(" [Ret %s]", resultSource))
		contextObserver(ctx, "async_retrieve", perfExtra1, perfExtra2, perfExtra3).Observe(elapsedSeconds)
		reqFinishChan <- 1
		defer close(reqFinishChan)
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

	if req.Destination != nil && req.Destination.DestinationType == pb.RetrieveDestinationType_COS &&
			req.RetrieveType != pb.RetrieveType_IMAGE {
		contextLog(ctx, fmt.Sprintf(" [InvalidDest]"))
		perfExtra1, perfExtra2 = "error", "invalid_dest"
		return &pb.RetrieveResponse{
			RetCode:	ERR_INVALID_ARGUMENT,
			DebugString:	fmt.Sprintf("Only image support COS destination"),
		}
	}

	var h RetrieveStatusHolder
	globalCacheDoneChan := make(chan int)
	realtimeCacheDoneChan := make(chan int)
	asyncCacheDoneChan := make(chan int)
	crawlAndParseChan := make(chan int)
	doneChan := make(chan int, 1)

	updateMinReturnTime := func(t int32) {
		for {
			old := h.minReturnTimeMs.Load()

			if old > 0 && old < t {
				return
			}

			if h.minReturnTimeMs.CompareAndSwap(old, t) {
				return
			}
		}
	}

	e.asyncWorkerCall("retrieve_cycle", func() {
		defer close(doneChan)

		barrier := 3
		for barrier > 0 {
			select {
			case <-globalCacheDoneChan:
				globalCacheDoneChan = nil
				barrier --
			case <-realtimeCacheDoneChan:
				realtimeCacheDoneChan = nil
				barrier --
			case <-asyncCacheDoneChan:
				asyncCacheDoneChan = nil
				// Evaluate cache entity
				ret := h.asyncCacheResult.Load()
				redo := e.needRedoCrawlAndParse(ctx, req, ret);
				if ret != nil && ret.CachedParses != nil {
					for _, parse := range ret.CachedParses {
						if parse.Success {
							elapsedMs := int32(time.Since(startTime).Milliseconds())
							updateMinReturnTime(elapsedMs)
							break
						}
					}
				}
				if redo {
					e.crawlAndParse(&CrawlAndParseTask{
						Ctx:		context.WithoutCancel(ctx),
						Req:		req,
						CacheEntity:	ret,
						TimeoutMs:	60 * 1000,
						BypassCache:	true,
						Callback:	func(res *CrawlAndParseResult, err error) {
							defer func() { close(crawlAndParseChan) }()
							if res != nil {
								if res.Res != nil && res.Res.Result != nil && len(res.Res.Result.Content) > 0{
									elapsedMs := int32(time.Since(startTime).Milliseconds())
									updateMinReturnTime(elapsedMs)
								}
								h.crawlAndParseResult.Store(res)
							}
						},
					})
				} else {
					e.asyncWorkerCall("finalize", func() {
						e.doCacheTidy(ctx, req, ret)
						close(crawlAndParseChan)
					})
				}
			case <-crawlAndParseChan:
				crawlAndParseChan = nil
				barrier --
			}
		}

		doneChan <- 1
		<-reqFinishChan

		glog.Info(contextLogStr(ctx))

		// On dev pod do not refresh cache or send kafka message
		if *IsDev {
			return
		}

		// Refresh cache
		crResult := h.crawlAndParseResult.Load()
		if crResult != nil && !req.BypassCache {
			e.doCacheRefresh(ctx, req, h.asyncCacheResult.Load(), crResult)
		}

		// Send to kafka
		metadata := &pb.RetrieveRequestLogMetadata {
			ResultSource:		resultSource,
			MinReturnTime:		h.minReturnTimeMs.Load(),
		}

		if p := h.asyncCacheResult.Load(); p != nil {
			// Clear useless fields to save space
			p.Url = ""
			for _, c := range p.CachedCrawls {
				c.Content = []byte{}
			}
			for _, c := range p.CachedParses {
				c.Result = nil
			}
			metadata.CacheContent = []*pb.CacheEntity { p }
		}

		if crResult != nil {
			if c := crResult.CrawlContext; c != nil {
				c.Content = []byte{}
				metadata.Crawls = []*pb.CrawlContext { c }
			}

			if c := crResult.ParseContext; c != nil {
				c.Result = nil
				metadata.Parses = []*pb.ParseContext{ c }
			}
		}

		metadataBytes, err := protojson.MarshalOptions {
			EmitUnpopulated: true,
		}.Marshal(metadata)
		if err != nil {
			glog.Error("Failed to marshal kafka message!")
			metadataBytes = []byte{}
		}

		rlog := &pb.RetrieveRequestLog {
			BizDef:			req.BizDef,
			RequestId:		req.RequestId,
			Url:			req.Url,
			RetrieveType:		pb.RetrieveType_name[int32(req.RetrieveType)],
			BypassCache:		utils.BoolToInt(req.BypassCache),
			ForeignHint:		utils.BoolToInt(req.ForeignHint),
			RequestTimestampMs:	startTime.UnixMilli(),
			TimeoutMs:		req.TimeoutMs,
			TimecostMs:		int32(time.Since(startTime).Milliseconds()),
			RetCode:		h.retrieveResponse.Load().RetCode,
			Metadata:		string(metadataBytes),
            SearchType:     req.SearchType,
		}

		msg, err := protojson.MarshalOptions {
			EmitUnpopulated: true,
		}.Marshal(rlog)
		if err != nil {
			glog.Error("Failed to marshal kafka message!")
		} else {
			e.kafkaProduce(&KafkaProduceTask{
				Topic: "goliath_retrieve_info",
				Message: msg,
			})
		}
	})

	if !req.BypassCache {
		// Process global cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("crawl:%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				defer func() { close(globalCacheDoneChan) }()
				elapsedSeconds := time.Since(startTime).Seconds()
				if err != nil || ret == "none" {
					contextObserver(ctx, "global_cache", "miss", "", "").Observe(elapsedSeconds)
					contextLog(ctx, fmt.Sprintf(" [GCache miss]"))
					return
				}
				contextObserver(ctx, "global_cache", "hit", "", "").Observe(elapsedSeconds)
				contextLog(ctx, fmt.Sprintf(" [GCache hit %f]", elapsedSeconds))

				parseResult := &ppb.ParseContentResponse{}
				err = proto.Unmarshal([]byte(ret), parseResult)
				if err != nil {
					glog.Error("Failed to unmarshal proto from global cache, Url = ", req.Url)
				} else {
					h.globalCacheResult.Store(parseResult)
					updateMinReturnTime(int32(elapsedSeconds * 1000))
				}
			},
		})

		// Process realtime cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("crawl_rt:%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				defer func() { close(realtimeCacheDoneChan) }()
				elapsedSeconds := time.Since(startTime).Seconds()
				if err != nil || ret == "none" {
					contextObserver(ctx, "realtime_cache", "miss", "", "").Observe(elapsedSeconds)
					contextLog(ctx, fmt.Sprintf(" [RCache miss %f]", elapsedSeconds))
					return
				}
				contextObserver(ctx, "realtime_cache", "hit", "", "").Observe(elapsedSeconds)
				contextLog(ctx, fmt.Sprintf(" [RCache hit %f]", elapsedSeconds))

				parseResult := &ppb.ParseContentResponse{}
				err = proto.Unmarshal([]byte(ret), parseResult)
				if err != nil {
					glog.Error("Failed to unmarshal proto from realtime cache, Url = ", req.Url)
				} else {
					h.realtimeCacheResult.Store(parseResult)
					updateMinReturnTime(int32(elapsedSeconds * 1000))
				}
			},
		})

		// Process async cache
		e.readCache(&CacheReaderTask{
			Key:		fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(req.Url))),
			Callback:	func(ret string, err error) {
				var perfExtra1, perfExtra2, perfExtra3 string
				elapsedSeconds := time.Since(startTime).Seconds()
				defer func() {
					close(asyncCacheDoneChan)
					contextObserver(ctx, "async_cache", perfExtra1, perfExtra2, perfExtra3).Observe(elapsedSeconds)
				}()
				if err != nil {  // Cache miss
					perfExtra1 = "miss"
					contextLog(ctx, fmt.Sprintf( "[ACache miss %f]", elapsedSeconds))
					return
				}
				perfExtra1 = "hit"
				perfExtra2 = "fake"
				contextLog(ctx, fmt.Sprintf( "[ACache hit %f]", elapsedSeconds))

				cacheEntity := &pb.CacheEntity{}
				err = proto.Unmarshal([]byte(ret), cacheEntity)
				if err != nil {
					glog.Error("Failed to unmarshal protobuf for async cache, Url = ", req.Url)
				} else {
					for _, parse := range cacheEntity.CachedParses {
						if parse.Success {
							updateMinReturnTime(int32(elapsedSeconds * 1000))
							perfExtra2 = "real"
							if len(parse.Result.Content) == 0 {
								perfExtra3 = "zero_content"
							} else if len(parse.Result.Content) <= 150 {
								perfExtra3 = "short_content"
							} else {
								perfExtra3 = "long_content"
							}
							break
						}
					}
					h.asyncCacheResult.Store(cacheEntity)
				}
			},
		})
	} else {
		globalCacheDoneChan <- 1
		realtimeCacheDoneChan <- 1
		asyncCacheDoneChan <- 1
	}

	timeoutMs := req.TimeoutMs - 10
	timer := time.NewTicker(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	fallback := func() *pb.RetrieveResponse {
		var r *pb.RetrieveResponse
		if p := h.crawlAndParseResult.Load();
			p != nil && p.ParseContext != nil && p.ParseContext.Success {
			perfExtra2 = "crawl_and_parse"
			resultSource = "CrawlAndParse"
			r = &pb.RetrieveResponse{
				Result:	p.ParseContext.Result,
			}
		} else if p := e.convertCacheEntity(ctx, req, h.asyncCacheResult.Load()); p != nil {
			perfExtra2 = "async_cache"
			resultSource = "ACache"
			r = p
		} else if p := h.realtimeCacheResult.Load(); p != nil {
			perfExtra2 = "realtime_cache"
			resultSource = "RCache"
			r = convertParseResult(ctx, p)
		} else if p := h.globalCacheResult.Load(); p != nil {
			perfExtra2 = "global_cache"
			resultSource = "GCache"
			r = convertParseResult(ctx, p)
		}

		if r != nil {
			if len(r.Result.Content) == 0 {
				perfExtra3 = "zero_content"
			} else if len(r.Result.Content) <= 150 {
				perfExtra3 = "short_content"
			} else {
				perfExtra3 = "long_content"
			}
			return r
		}

		perfExtra1 = "error"
		return &pb.RetrieveResponse{
			RetCode:	ERR_INTERNAL_ERROR,
		}
	}

	select {
	case <-doneChan:
	case <-timer.C:
	}

	res := fallback()
	h.retrieveResponse.Store(res)

	if req.Destination != nil && res != nil && res.RetCode == 0 && res.Result != nil && res.Result.Content != nil &&
			req.Destination.DestinationType == pb.RetrieveDestinationType_COS {
		bucket := req.Destination.CosBucket
		path := req.Destination.CosPath
		content := res.Result.Content

		err := uploadToCos(bucket, path, content)
		if err != nil {
			glog.Error("Failed to write to COS: ", err)
		}
	}

	// Whether Publish Time is filled
	if res != nil && res.RetCode == 0 && res.Result != nil {
		if len(res.Result.PublishTime) > 0 {
			contextCounter(ctx, "retrieve_have_publish_time", "yes", "", "").Inc()
		} else {
			contextCounter(ctx, "retrieve_have_publish_time", "no", "", "").Inc()
		}
	}

	return res
}

func (e *Executor) crawlAndParse(r *CrawlAndParseTask) {
	e.asyncWorkerCall("crawl_and_parse", func() {
		res, err := e.doCrawlAndParse(r)
		r.Callback(res, err)
	})
}

func (e *Executor) syncRetrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	res, _ := e.doCrawlAndParse(&CrawlAndParseTask {
		Ctx:		ctx,
		Req:		req,
		BypassCache:	req.BypassCache,
		TimeoutMs:	req.TimeoutMs,
	})
	return res.Res
}

func (e *Executor) Retrieve(ctx context.Context, req *pb.RetrieveRequest) *pb.RetrieveResponse {
	defer func() {
		if r := recover(); r != nil {
			glog.Error("Panic: ", r, "\n", string(debug.Stack()))
		}
		glog.Flush()
	}()
	contextLog(ctx, fmt.Sprintf(" [BizDef %s]", req.BizDef))
	contextLog(ctx, fmt.Sprintf(" [URL %s]", req.Url))

	// TODO(wanghaocheng): 
	// Sample 10% traffic to dev pods for debugging purposes
	// Need to be replaced by more canonical approach
	if !*IsDev {
		sample := rand.Intn(100) < 10

		if sample {
			e.asyncWorkerCall("redirect", func() { e.invokeDevRetrieve(ctx, req) })
		} 
	}
	return e.asyncRetrieve(ctx, req)
}

func (e *Executor) doCrawlAndParse(t *CrawlAndParseTask) (*CrawlAndParseResult, error) {
	startTime := time.Now()

	ctx := t.Ctx
	req := t.Req
	bypassCache := t.BypassCache
	timeoutMs := t.TimeoutMs

	perfExtra1, perfExtra2, perfExtra3 := "success", "", ""
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "retrieve", perfExtra1, perfExtra2, perfExtra3).Observe(elapsedSeconds)
	}()

	if timeoutMs == 0 {
		timeoutMs = req.TimeoutMs
	}

	var crawlContext *pb.CrawlContext

	bypassCache = bypassCache || req.BypassCache
	if !bypassCache {
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
	} else {
		if (req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_IMAGE) && t.CacheEntity != nil {
			for _, c := range t.CacheEntity.CachedCrawls {
				if c != nil && c.Success && c.ContentLen > 0 {
					if len(c.ContentCosnPath) > 0 {
						content, err := downloadFromCos(cosCrawlBucket, c.ContentCosnPath)
						if err != nil {
							glog.Error("Failed to download content from cosn: path = ", c.ContentCosnPath)
						} else {
							c.Content = content
						}
					}

					if len(c.Content) > 0 {
						glog.Info("Cache entity assigned.")
						crawlContext = c
						break
					} else {
						glog.Error("Found cache entity with zero length, url = ", req.Url)
					}
				}
			}
		}
	}

	// Reinvoke crawl crawl
	if crawlContext == nil {
		var err error
		crawlContext, err = e.invokeCrawl(ctx, req)
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
	}

	// No need to parse for image
	if req.RetrieveType == pb.RetrieveType_IMAGE {
		return convertImageCrawlContext(ctx, req, crawlContext), nil
	}

	elapsed := int32(time.Since(startTime).Milliseconds())
	if elapsed >= timeoutMs {
		perfExtra1 = "timeout"
		contextLog(ctx, fmt.Sprintf(" [CrawlFailed %s]", crawlContext.ErrorMessage))
		return &CrawlAndParseResult{
			Res:	&pb.RetrieveResponse{
				RetCode:	ERR_TIMEOUT,
			},
			CrawlContext:	crawlContext,
		}, nil
	}
	parsed, parseContext, err := e.invokeParse(ctx, req, crawlContext, timeoutMs - elapsed)
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

	if !bypassCache {
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

	r := &pb.RetrieveResponse {
		Result: parseContext.Result,
	}
	ct := pb.ContentType_TYPE_UNKNOWN
	if isPDF(req, crawlContext.ContentType) {
		ct = pb.ContentType_TYPE_PDF
	} else {
		ct = pb.ContentType_TYPE_WEB_PAGE
	}
	r.Result.SourceContentType = ct

	return &CrawlAndParseResult{
		Res:		r,
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
