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
	url := "https://mogua.co/static_analyzer/?name=base.apk&checksum=fb11f2ab14c0334e0688454e1eaf5c66&type=apk"
	//url := "https://mmedispa.com/2025/04/21/toronto-2025-reveal-simple-effective-acne-removal-secrets/"
	//url := "https://zhidao.baidu.com/question/1708281467644467820.html"
	key := fmt.Sprintf("Goliath|Async|%d", city.Hash64([]byte(url)))
	//key = "Goliath|Async|18207729575868377158"
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
