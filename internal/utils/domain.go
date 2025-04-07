package utils

import (
	"fmt"
	nu "net/url"
	"strings"
)

func ExtractDomain(url string) (string, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return "", fmt.Errorf("Not valid URL: Only http or http supported")
	}

	u, err := nu.Parse(url)
	if err != nil {
		return "", fmt.Errorf("Not valid URL: ", err)
	}

	domain := u.Host
	if strings.Contains(domain, ":") {
		domain, _, _ = strings.Cut(domain, ":")
	}

	return domain, nil
}
