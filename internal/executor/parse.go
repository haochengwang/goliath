package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	pb "example.com/goliath/proto/goliath/v1"
	ppb "example.com/goliath/proto/parser/v1"
)

func (e *Executor) invokeParse(ctx context.Context, req *pb.RetrieveRequest, crawlContext *pb.CrawlContext, parseTimeoutMs int32) (*ppb.ParseContentResponse, *pb.ParseContext, error) {
	if req.RetrieveType == pb.RetrieveType_MARKDOWN_RICH {
		return e.invokeJinaParse(ctx, req, crawlContext, parseTimeoutMs)
	} else {
		return e.invokeNormalParse(ctx, req, crawlContext, parseTimeoutMs)
	}
}

type JinaParseResult struct {
	Title	string	`json:"title"`
	Url	string	`json:"url"`
	Content	string	`json:"content"`
}

func (e *Executor) invokeJinaParse(ctx context.Context, req *pb.RetrieveRequest, crawlContext *pb.CrawlContext, parseTimeoutMs int32) (*ppb.ParseContentResponse, *pb.ParseContext, error) {
	startTime := time.Now()

	parserKey := "BJ-JINA"
	postData, _ := json.Marshal(map[string]string {
		"html": string(crawlContext.Content),
		"respondWith": "json",
	})
	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/%s",
		e.parserAddrs[parserKey], req.Url), bytes.NewBuffer(postData))
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "parse", "error", parserKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Create request failed: Err = %v", err)
		return nil, &pb.ParseContext {
			ParserKey:		parserKey,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
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
		contextObserver(ctx, "parse", "error", parserKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed: Err = %v", err)
		return nil, &pb.ParseContext {
			ParserKey:		parserKey,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "parse", "error_status_code", parserKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("HTTP Failed: StatusCode = %v", resp.StatusCode)
		return nil, &pb.ParseContext {
			ParserKey:		parserKey,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}

	maxContentLength := int64(4 * 1024 * 1024)
	bodyReader := io.LimitReader(resp.Body, maxContentLength)
	rawBody, _ := io.ReadAll(bodyReader)

	jinaParseResult := JinaParseResult{}
	err = json.Unmarshal(rawBody, &jinaParseResult)
	if err != nil {
		elapsedSeconds := time.Since(startTime).Seconds()
		contextObserver(ctx, "parse", "error_response_format", parserKey, "").Observe(elapsedSeconds)
		err = fmt.Errorf("Parse json failed, err = %v", err)
		return nil, &pb.ParseContext {
			ParserKey:		parserKey,
			ParseTimestampMs:	startTime.UnixMilli(),
			ParseTimecostMs:	int64(elapsedSeconds * 1000),
			Success:		false,
			ErrorMessage:		err.Error(),
		}, err
	}
	result := ppb.ParseContentResponse{
		Title:		[]byte(jinaParseResult.Title),
		Url:		jinaParseResult.Url,
		Content:	[]byte(jinaParseResult.Content),
	}
	// If PublishTime is not parsed, use last modified
	if len(result.PublishTime) == 0 {
		result.PublishTime = crawlContext.LastModified
	}
	elapsedSeconds := time.Since(startTime).Seconds()
	return &result, &pb.ParseContext{
		ParserKey:		parserKey,
		ParseTimestampMs:	startTime.UnixMilli(),
		ParseTimecostMs:	int64(elapsedSeconds * 1000),
		Success:		true,
		Result:			&pb.RetrieveResult{
			Title:		[]byte(jinaParseResult.Title),
			Url:		jinaParseResult.Url,
			Content:	[]byte(jinaParseResult.Content),
		},
		ContentLen:		int64(len(result.Content)),
	}, err
}


func (e *Executor) invokeNormalParse(ctx context.Context, req *pb.RetrieveRequest, crawlContext *pb.CrawlContext, parseTimeoutMs int32) (*ppb.ParseContentResponse, *pb.ParseContext, error) {
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

