package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	//"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang/glog"
	pb "example.com/goliath/proto/goliath/v1"
	cpb "example.com/goliath/proto/crawler/v1"
)

func (e *Executor) invokeCrawl(ctx context.Context, req *pb.RetrieveRequest) (*pb.CrawlContext, error) {
	// TODO: parallel crawl from multiple region
	region := "BJ"
	timeoutMs := int32(60 * 1000)
	maxContentLen := int32(30 * 1024 * 1024)
	if req.ForeignHint {
		// TODO(wanghaocheng): Avoid heavy traiffc via Tokyo
		if strings.HasSuffix(req.Url, "pdf") {
			region = "SV-ALI"
		} else if rand.Intn(100) < 40 {
			region = "SV-ALI"
		} else {
			region = "TOK"
			timeoutMs = int32(10 * 1000)
			maxContentLen = int32(10 * 1024 * 1024)
		}
	// TODO: whitelist this
	} else if strings.HasPrefix(req.Url, "https://zhuanlan.zhihu.com") && rand.Intn(100) < 5 {
		region = "BJ:RENDER"
	} else if rand.Intn(100) < 60 {
		region = "BJNEW"
	}
	if region == "BJ:RENDER" {
		return e.invokeRenderCrawl(ctx, req, region, timeoutMs)
	//} else if region == "SV-ALI-NGINX" {
	//	return e.invokeNginxCrawl(ctx, req, region, timeoutMs)
	} else {
		return e.invokeNormalCrawl(ctx, req, region, timeoutMs, maxContentLen)
	}
}

func (e *Executor) invokeNginxCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerKey string, timeoutMs int32) (*pb.CrawlContext, error) {
	startTime := time.Now()

	maxContentLength := int64(30 * 1024 * 1024)

	request, err := http.NewRequest("GET", req.Url, nil)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Create request failed: Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerKey,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	request.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 user agent to emulate pc")
	request.Header.Set("Accept", "*/*")

	proxyURL, _ := url.Parse(fmt.Sprintf("http://%s", e.crawlerAddrs[crawlerKey]))
	client := &http.Client{
		//Timeout: 	time.Duration(timeoutMs) * time.Millisecond,
		Timeout:	100000 * time.Second,
		Transport: &http.Transport{
			Proxy:			http.ProxyURL(proxyURL),
			// TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
			// Connection pool settings
			IdleConnTimeout:	90 * time.Second,
			DisableCompression:	false,
		},
	}

	resp, err := client.Do(request)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed: Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerKey,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	contentEncoding := resp.Header.Get("Content-Encoding")

	if resp.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_status_code", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed: StatusCode = %v", resp.StatusCode)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerKey,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			ContentType:		contentType,
			ContentEncoding:	contentEncoding,
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if int64(resp.ContentLength) > maxContentLength {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_content_too_large", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Content Length too large, Length = %d", resp.ContentLength)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerKey,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			ContentType:		contentType,
			ContentEncoding:	contentEncoding,
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	// mediaType, _, err := mime.ParseMediaType(contentType)
	// if err != nil {
	//	glog.Error("Failed to parse media type: ", contentType)
	// }

	bodyReader := io.LimitReader(resp.Body, maxContentLength)
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_read_body", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed to read content, Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerKey,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			ContentType:		contentType,
			ContentEncoding:	contentEncoding,
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerKey, "").Observe(elapsedSeconds)
	return &pb.CrawlContext {
		CrawlerKey:		crawlerKey,
		CrawlTimestampMs:	startTime.UnixMilli(),
		CrawlTimecostMs:	int64(elapsedSeconds * 1000),
		ContentType:		contentType,
		ContentEncoding:	contentEncoding,
		Content:		body,
		ContentLen:		int64(len(body)),
		Success:		true,
	}, nil
}

func (e *Executor) invokeRenderCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string, timeoutMs int32) (*pb.CrawlContext, error) {
	startTime := time.Now()

	url := e.crawlerAddrs[crawlerRegion]
	data, err := json.Marshal(map[string]interface{} {
		"url":		req.Url,
	})
	client := &http.Client{}
	r, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(data)))
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Create request failed: Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	resp, err := client.Do(r)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_http_%d", resp.StatusCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: StatusCode = %d", resp.StatusCode)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_io", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Read body failed: Err = %v", err)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if len(body) == 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "empty content", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: no content")
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	glog.Info(len(body))

	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerRegion, "").Observe(elapsedSeconds)
	return &pb.CrawlContext {
		CrawlerKey:		crawlerRegion,
		CrawlTimestampMs:	startTime.UnixMilli(),
		CrawlTimecostMs:	int64(elapsedSeconds * 1000),
		Success:		true,
		HttpCode:		int32(resp.StatusCode),
		Content:		body,
		ContentLen:		int64(len(body)),
	}, nil
}

func (e *Executor) invokeNormalCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string, timeoutMs int32, maxContentLen int32) (*pb.CrawlContext, error) {
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
	innerCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs) * time.Millisecond)
	defer cancel()

	var estimatedParseTimeMs int32 = 100
	if req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_VIDEO_SUMMARY {
		estimatedParseTimeMs = 1000
	}
	r, err := c.Download(innerCtx, &cpb.CrawlRequest{
		Url:			req.Url,
		RequestType:		cpb.RequestType_GET,
		UaType:			cpb.UAType_PC,
		CrawlType:		cpb.CrawlType_HTML,
		TimeoutMs:		timeoutMs - estimatedParseTimeMs,
		MaxContentLength:	maxContentLen,
		TaskInfo:	&cpb.TaskInfo{
			TaskName:	req.BizDef,
		},
	})

	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: Err = %v", err)
		return &pb.CrawlContext{
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if r.ErrCode != cpb.ERROR_CODE_SUCCESS {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_%s", r.ErrCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: ErrCode = %d", r.ErrCode)
		return &pb.CrawlContext{
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	if r.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", fmt.Sprintf("error_http_%d", r.StatusCode), crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed: StatusCode = %d", r.StatusCode)
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
			HttpCode:		r.StatusCode,
		}, err
	}

	if len(r.Content) == 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "empty_content", crawlerRegion, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Crawl failed, no content")
		return &pb.CrawlContext {
			CrawlerKey:		crawlerRegion,
			CrawlTimestampMs:	startTime.UnixMilli(),
			CrawlTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
			HttpCode:		r.StatusCode,
			Content:		r.Content,
			ContentLen:		0,
			ContentType:		r.ContentType,
			ContentEncoding:	r.Encoding,
		}, err
	}

	//glog.Info("Crawl succeed, Url = ", req.Url, ", crawlPod = ", r.CrawlPodIp)
	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerRegion, "").Observe(elapsedSeconds)
	return &pb.CrawlContext {
		CrawlerKey:		crawlerRegion,
		CrawlTimestampMs:	startTime.UnixMilli(),
		CrawlTimecostMs:	int64(elapsedSeconds * 1000),
		Success:		true,
		HttpCode:		r.StatusCode,
		Content:		r.Content,
		ContentLen:		int64(len(r.Content)),
		ContentType:		r.ContentType,
		ContentEncoding:	r.Encoding,
	}, nil
}
