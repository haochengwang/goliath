package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-faster/city"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "example.com/goliath/proto/goliath/v1"
)

func main() {
	flag.Parse()

	redisClient := redis.NewClient(&redis.Options{
		Addr:		"10.3.0.103:6379",
		Password:	"2023@Ystech",
		DB:		0,
	})
	url := "https://www.msn.cn/zh-cn/news/other/%%E4%BA%8B%%E5%85%B3%%E5%8D%8A%%E5%AF%BC%%E4%BD%93-%%E5%%8E%%9F%%E4%BA%A7%%E5%9C%B0-%%E8%AE%A4%%E5%AE%9A%%E8%A7%84%%E5%88%99-%%E4%B8%AD%%E5%9B%BD%%E5%8D%8A%%E5%AF%BC%%E4%BD%93%%E8%A1%8C%%E4%B8%9A%%E5%8D%%8F%%E4%BC%9A%%E7%B4%A7%%E6%80%A5%%E6%%8F%90%%E9%86%92/ar-AA1CIgBx?ocid=BingHp01&cvid=71285b6335f940f39bbbfef72738fb1d&ei=10"
	//url := "https://mmedispa.com/2025/04/21/toronto-2025-reveal-simple-effective-acne-removal-secrets/"
	//url := "https://zhidao.baidu.com/question/1708281467644467820.html"
	key := fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(url)))
	key = "Goliath|Async|18207729575868377158"
	ret, err := redisClient.Get(context.Background(), key).Result()

	if err != nil {  // Cache miss
		panic(err)
	}

	cacheEntity := &pb.CacheEntity{}
	err = proto.Unmarshal([]byte(ret), cacheEntity)
	if err != nil {
		panic(err)
	}

	c := []string{}
	for _, parse := range cacheEntity.CachedParses {
		if parse.Result != nil {
			c = append(c, string(parse.Result.Content))
			parse.Result.Content = []byte{}
		}
	}
	opts := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}
	jsonBytes, err := opts.Marshal(cacheEntity)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))

	for _, result := range c {
		fmt.Println(result)
	}
}
