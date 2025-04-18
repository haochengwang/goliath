package executor

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "example.com/goliath/proto/goliath/v1"
	"example.com/goliath/internal/utils"
)

var (
	ctxReqIdKey = "reqid"
	ctxDomainKey = "domain"
	ctxBizDefKey = "bizdef"
)

func generateReqId() string {
	now := time.Now()
	return fmt.Sprintf("%04d%02d%02d%02d%02d%02d%06d", now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.UnixNano() % 1000000)
}

func GoliathInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	bizdef := ""
	reqId := ""
	if r, ok := req.(*pb.RetrieveRequest); ok {
		bizdef = fmt.Sprintf("%s_%s", r.BizDef, r.RetrieveType.String())
		reqId = r.RequestId

		domain, err := utils.ExtractDomain(r.Url)
		if err != nil && len(domain) > 0 {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		ctx = context.WithValue(ctx, ctxDomainKey, domain)
	}
	if r, ok := req.(*pb.SearchRequest); ok {
		bizdef = r.BizDef
		reqId = r.RequestId
	}
	if len(bizdef) == 0 {
		return nil, status.Error(codes.Unauthenticated, "No biz_def")
	}
	if len(reqId) == 0 {
		reqId = generateReqId()
	}
	ctx = context.WithValue(ctx, ctxBizDefKey, bizdef)
	ctx = context.WithValue(ctx, ctxReqIdKey, reqId)

	res, err := handler(ctx, req)
	if err != nil {
		return res, err
	}

	if r, ok := res.(*pb.RetrieveResponse); ok {
		r.RequestId = reqId
	}
	return res, nil
}
