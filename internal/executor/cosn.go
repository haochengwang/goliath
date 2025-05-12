package executor

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	//"strings"
	"time"

	"github.com/golang/glog"
	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/tencentyun/cos-go-sdk-v5/debug"
)

func downloadFromCos(path string) ([]byte, error) {
	u, _ := url.Parse("https://web-crawl-1319140468.cos.ap-beijing.myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	c := cos.NewClient(b, &http.Client{
		Timeout: 100 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID: os.Getenv("COSN_SECRET_ID"),
			SecretKey: os.Getenv("COSN_SECRET_KEY"),
			Transport: &debug.DebugRequestTransport{
				RequestHeader:  false,
				RequestBody:    false,
				ResponseHeader: false,
				ResponseBody:   false,
			},
		},
	})

	resp, err := c.Object.Get(
		context.Background(), path, nil,
	)
	if err != nil {
		return nil, err
	}
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	glog.Info("Read from COSN path = ", path, ", content_len = ", len(content))
	return content, err

}

func uploadToCos(path string, content []byte) error {
	if len(content) == 0 {
		glog.Error("Upload to cos content size = 0! path = ", path)
		return nil
	}
	glog.Info("Writing to COSN path = ", path, ", content_len = ", len(content))
	u, _ := url.Parse("https://web-crawl-1319140468.cos.ap-beijing.myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	c := cos.NewClient(b, &http.Client{
		Timeout: 100 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID: os.Getenv("COSN_SECRET_ID"),
			SecretKey: os.Getenv("COSN_SECRET_KEY"),
			Transport: &debug.DebugRequestTransport{
				RequestHeader:  false,
				RequestBody:    false,
				ResponseHeader: false,
				ResponseBody:   false,
			},
		},
	})

	_, err := c.Object.Put(
		context.Background(), path, bytes.NewReader(content), nil,
	)
	return err

	/*
	opt.OptIni = &cos.InitiateMultipartUploadOptions{
		nil,
		&cos.ObjectPutHeaderOptions{
			Listener: &cos.DefaultProgressListener{},
		},
	}
	v, _, err = c.Object.Upload(
		context.Background(), "gomulput1G", "./test1G", opt,
	)
	logStatus(err)
	fmt.Printf("Case2 done, %v\n", v)
	*/

}
