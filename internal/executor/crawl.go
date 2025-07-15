package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	//"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/golang/glog"
	pb "example.com/goliath/proto/goliath/v1"
	cpb "example.com/goliath/proto/crawler/v1"
)

// Rendered crawl by chenxiaoxi
type RenderCrawlResult struct {
	Code	int 					`json:"code"`
	Data	struct {
		Render	struct {
			Body	string 			`json:"body"`
		}					`json:"render"`
	}						`json:"data"`
}

// Offline cached doc by chenxiaoxi
type SpiderCachedDoc struct {
	Code	int					`json:"code"`
	Data	struct {
		Html	struct {
			Url		string		`json:"url"`
			StatusCode	int		`json:"status_code"`
			Headers		map[string][]string	`json:"headers"`
			Body	string 			`json:"body"`
		}					`json:"html"`
	}						`json:"data"`
}

type HttpCrawlHandler interface {
	GetHttpRequest(r *pb.RetrieveRequest)	(*http.Request, error)
	Parse([]byte)				(interface{}, error)
	GetResultCode(interface{})		int
	GetCrawledBody(interface{})		string
	GetContentType(interface{})		string
	GetContentEncoding(interface{})		string
}

type RenderCrawlHandler struct{}
type SpiderCachedCrawlHandler struct{}

func (h RenderCrawlHandler) GetHttpRequest(r *pb.RetrieveRequest) (*http.Request, error) {
	postData, _ := json.Marshal(map[string]string {
		"url": r.Url,
	})
	return http.NewRequest("POST", "http://10.3.32.133:8888/api/v1/doc/fetch/render/",
		bytes.NewBuffer(postData))
}

func (h RenderCrawlHandler) Parse(rawBody []byte) (interface{}, error) {
	retBody := RenderCrawlResult{}
	err := json.Unmarshal(rawBody, &retBody)
	if err != nil {
		return nil, err
	} else {
		return &retBody, nil
	}
}

func (h RenderCrawlHandler) GetResultCode(r interface{}) int {
	return r.(*RenderCrawlResult).Code
}

func (h RenderCrawlHandler) GetCrawledBody(r interface{}) string {
	return r.(*RenderCrawlResult).Data.Render.Body
}

func (h RenderCrawlHandler) GetContentType(r interface{}) string {
	return ""
}

func (h RenderCrawlHandler) GetContentEncoding(r interface{}) string {
	return ""
}

func (h SpiderCachedCrawlHandler) GetHttpRequest(req *pb.RetrieveRequest) (*http.Request, error) {
	params := url.Values{}
	params.Add("url", req.Url)
	url := fmt.Sprintf("http://10.3.32.133:8888/api/v1/doc/html/?%s", params.Encode())
	return http.NewRequest("GET", url, nil)
}

func (h SpiderCachedCrawlHandler) Parse(rawBody []byte) (interface{}, error) {
	retBody := SpiderCachedDoc{}
	err := json.Unmarshal(rawBody, &retBody)
	if err != nil {
		return nil, err
	} else {
		return &retBody, nil
	}
}

func (h SpiderCachedCrawlHandler) GetResultCode(r interface{}) int {
	return r.(*SpiderCachedDoc).Code
}

func (h SpiderCachedCrawlHandler) GetCrawledBody(r interface{}) string {
	return r.(*SpiderCachedDoc).Data.Html.Body
}

func (h SpiderCachedCrawlHandler) GetContentType(r interface{}) string {
	return ""
}

func (h SpiderCachedCrawlHandler) GetContentEncoding(r interface{}) string {
	return ""
}

func (e *Executor) invokeCrawl(ctx context.Context, req *pb.RetrieveRequest) (*pb.CrawlContext, error) {
	foreign := req.ForeignHint
	parsedUrl, err := url.Parse(req.Url)
	if err != nil {
		glog.Error("Failed to parse URL: ", parsedUrl)
	}

	region := "BJ"
	timeoutMs := int32(60 * 1000)
	// No blacklist or foreign whitelist for MARKDOWN_RICH
	if req.RetrieveType == pb.RetrieveType_MARKDOWN_RICH {
		c, err := e.invokeHttpCrawl(ctx, req, SpiderCachedCrawlHandler{}, "BJ-SPIDER", timeoutMs)
		if c.Success {
			return c, err
		}
		return e.invokeHttpCrawl(ctx, req, RenderCrawlHandler{}, "BJ-RENDER", timeoutMs)
	}

	for _, domain := range(e.conf.BlacklistDomain) {
		if parsedUrl.Hostname() == domain {
			return e.invokeHttpCrawl(ctx, req, SpiderCachedCrawlHandler{}, "BJ-SPIDER", timeoutMs)
		}
	}

	for _, domain := range(e.conf.ForeignCrawlDomain) {
		if parsedUrl.Hostname() == domain && !foreign {
			glog.Info("URL foreign crawl rewrite: ", req.Url)
			foreign = true
		}
	}

	maxContentLen := int32(30 * 1024 * 1024)
	retryTimes := int32(1)
	if foreign {
		// TODO(wanghaocheng): Avoid heavy traiffc via Tokyo
		if req.RetrieveType == pb.RetrieveType_IMAGE {
			region = "SV-ALI"
			retryTimes = 3
		} else if strings.HasSuffix(req.Url, "pdf") {
			region = "SV-ALI"
		} else if rand.Intn(100) < 40 {
			region = "SV-ALI"
		} else {
			region = "TOK"
			timeoutMs = int32(10 * 1000)
			maxContentLen = int32(10 * 1024 * 1024)
		}
	} else if rand.Intn(100) < 60 {
		region = "BJNEW"
	}
	if region == "BJ:RENDER" {
		return e.invokeRenderCrawl(ctx, req, region, timeoutMs)
	} else {
		return e.invokeNormalCrawl(ctx, req, region, timeoutMs, maxContentLen, retryTimes)
	}
}



// Invoke a http request for crawling target url
func (e *Executor) invokeHttpCrawl(ctx context.Context, req *pb.RetrieveRequest, h HttpCrawlHandler,
		crawlerKey string, timeoutMs int32) (*pb.CrawlContext, error) {
	startTime := time.Now()

	maxContentLength := int64(30 * 1024 * 1024)

	request, err := h.GetHttpRequest(req)
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
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		//Timeout: 	time.Duration(timeoutMs) * time.Millisecond,
		Timeout:	100000 * time.Second,
		Transport: &http.Transport{
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
	rawBody, err := io.ReadAll(bodyReader)
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

	retBody, err := h.Parse(rawBody)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_parse_body", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed to parse content, Err = %v", err)
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

	if h.GetResultCode(retBody) != 0 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "crawl", "error_ret_code", crawlerKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP non-zero result code, Code =", h.GetResultCode(retBody))
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

	body := h.GetCrawledBody(retBody)
	if h.GetContentType(retBody) != "" {
		contentType = h.GetContentType(retBody)
	}
	if h.GetContentEncoding(retBody) != "" {
		contentEncoding = h.GetContentEncoding(retBody)
	}

	elapsedSeconds := time.Since(startTime).Seconds()
	contextObserver(ctx, "crawl", "success", crawlerKey, "").Observe(elapsedSeconds)
	return &pb.CrawlContext {
		CrawlerKey:		crawlerKey,
		CrawlTimestampMs:	startTime.UnixMilli(),
		CrawlTimecostMs:	int64(elapsedSeconds * 1000),
		ContentType:		contentType,
		ContentEncoding:	contentEncoding,
		Content:		[]byte(body),
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

func tryFindPublishTime(req *pb.RetrieveRequest, headers string) string {
	re1, _ := regexp.Compile("/20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]/")
	if r := re1.FindString(req.Url); len(r) > 2 {
		glog.Info("URL match regexp1 when trying to parse publish time from url: ", req.Url)
		return r[1 : 5] + "-" + r[6 : 8] + "-" + r[9 : 11]
	}

	re2, _ := regexp.Compile("/20[0-9][0-9]/[0-1][0-9]/[0-3][0-9]/")
	if r := re2.FindString(req.Url); len(r) > 2 {
		glog.Info("URL match regexp2 when trying to parse publish time from url: ", req.Url)
		return r[1 : 5] + "-" + r[6 : 8] + "-" + r[9 : 11]
	}

	re3, _ := regexp.Compile("/20[0-9][0-9][0-1][0-9][0-3][0-9]/")
	if r := re3.FindString(req.Url); len(r) > 2 {
		glog.Info("URL match regexp3 when trying to parse publish time from url: ", req.Url)
		return r[1 : 5] + "-" + r[5 : 7] + "-" + r[7 : 9]
	}

	if i := strings.Index(headers, "Last-Modified: "); i != -1 {
		first := i + 15
		last := i + 15
		for ; last < len(headers) && headers[last] != '\n'; last ++ {}
		res := headers[first : last]
		glog.Info("Found last modified in header, url = ", req.Url, ", res = ", res)
		return res
	}

	return ""
}

func (e *Executor) invokeNormalCrawl(ctx context.Context, req *pb.RetrieveRequest, crawlerRegion string, timeoutMs int32, maxContentLen int32, retryTimes int32) (*pb.CrawlContext, error) {
	startTime := time.Now()

	var estimatedParseTimeMs int32 = 100
	if req.RetrieveType == pb.RetrieveType_PDF || req.RetrieveType == pb.RetrieveType_VIDEO_SUMMARY {
		estimatedParseTimeMs = 1000
	}

	ct := cpb.CrawlType_HTML
	if req.RetrieveType == pb.RetrieveType_IMAGE {
		ct = cpb.CrawlType_IMAGE
	}
	var r *cpb.CrawlResponse
	var err error
	for retry := int32(0); ; retry++ {
		conn, err := createGrpcConn(e.crawlerAddrs[crawlerRegion])
		if err != nil {
			glog.Error("Failed to create grpc connection, region = ", crawlerRegion, ", err = ", err)
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

		r, err = c.Download(innerCtx, &cpb.CrawlRequest{
			Url:			req.Url,
			RequestType:		cpb.RequestType_GET,
			UaType:			cpb.UAType_PC,
			CrawlType:		ct,
			TimeoutMs:		timeoutMs - estimatedParseTimeMs,
			MaxContentLength:	maxContentLen,
			TaskInfo:	&cpb.TaskInfo{
				TaskName:	req.BizDef,
			},
		})

		if err != nil {  // r == nil
			elapsedSeconds := time.Since(startTime).Seconds()
			contextObserver(ctx, "crawl", "error", crawlerRegion, "").Observe(elapsedSeconds)
			if retry < retryTimes {
				continue
			}
			err = fmt.Errorf("Crawl failed: Retry = %d, Err = %v", retry, err)

			return &pb.CrawlContext{
				CrawlerKey:		crawlerRegion,
				CrawlTimestampMs:	startTime.UnixMilli(),
				CrawlTimecostMs:	int64(elapsedSeconds * 1000),
				Success:		false,
				ErrorMessage:		err.Error(),
			}, err
		}

		break
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

	//glog.Info("Crawl succeed, Url = ", req.Url, ", crawlPod = ", r.CrawlPodIp, ", respHeaders = ", r.ResponseHeaders)

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
		LastModified:		tryFindPublishTime(req, r.ResponseHeaders),
	}, nil
}
