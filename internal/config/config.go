package config

import (
	"fmt"
)

type CrawlerEndpoint struct {
	Ip	string
	Port	int32
	Nginx	bool
}

type ParserEndpoint struct {
	Ip	string
	Port	int32
}

type Config struct {
	CrawlerEndpoints	map[string]CrawlerEndpoint
	ParserEndpoints		map[string]ParserEndpoint
}

func (c* Config) GetCrawlerEndpoints() {
	fmt.Println("Fuck")
}

func (c* Config) GetParserEndpoints() {
	fmt.Println("Fuck")
}
