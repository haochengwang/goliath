package config

import (
	"encoding/json"
	"os"

	"github.com/golang/glog"
)

type ServiceEndpoint struct {
	Host	string	`json:"host"`
	Port	int32	`json:"port"`
}

type JsonConfig struct {
	CrawlerEndpoints	map[string]ServiceEndpoint	`json:"crawlers"`
	ParserEndpoints		map[string]ServiceEndpoint	`json:"parsers"`
	BlacklistDomain		[]string			`json:"blacklist_domain"`
	ForeignCrawlDomain	[]string			`json:"foreign_crawl_domain"`
}

var (
	conf	JsonConfig
)

func LoadConfig(filename string) (*JsonConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		glog.Error("Failed to read file: ", filename, ", err: ", err)
		return nil, err
	}

	result := &JsonConfig{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		glog.Error("Failed to parse json: ", err)
		return nil, err
	}

	// For debug purposes
	s, err := json.Marshal(result)
	if err != nil {
		glog.Error("Failed to marshal json: ", err)
		return nil, err
	}
	glog.Info(s)

	return result, nil
}
