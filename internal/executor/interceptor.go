package executor

import (
	"context"
	"fmt"
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
	return ""
}

func GoliathInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	bizdef := ""
	reqId := ""
	if r, ok := req.(*pb.RetrieveRequest); ok {
		bizdef = r.BizDef
		reqId = r.RequestId

    		fmt.Println(r.String())
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
	return handler(ctx, req)
}
