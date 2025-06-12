package executor

import (
	"bytes"
	"context"
	"fmt"
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

func downloadFromCos(bucket string, path string) ([]byte, error) {
	cosUrl := fmt.Sprintf("https://%s.cos.ap-beijing.myqcloud.com", bucket)
	u, _ := url.Parse(cosUrl)
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
		glog.Error("Faield to read from COSN bucket = ", bucket, ", path = ", path)
		return nil, err
	}
	glog.Info("Read from COSN bucket = ", bucket, ", path = ", path, ", content_len = ", len(content))
	return content, err

}

func uploadToCos(bucket string, path string, content []byte) error {
	if len(content) == 0 {
		glog.Error("Upload to cos content size = 0! path = ", path)
		return nil
	}
	glog.Info("Writing to COSN bucket = ", bucket, ", path = ", path, ", content_len = ", len(content))
	cosUrl := fmt.Sprintf("https://%s.cos.ap-beijing.myqcloud.com", bucket)
	u, _ := url.Parse(cosUrl)
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
	if err != nil {
		glog.Error("Failed to write to COSN bucket = ", bucket, ", path = ", path)
	}
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
