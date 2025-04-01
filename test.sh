grpcurl -import-path ./proto -proto ./proto/goliath/v1/goliath.proto -plaintext -d '{"url": "http://www.sohu.com/"}' 127.0.0.1:9023 goliath.GoliathPortal.Retrieve
