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
	ctxDomainKey = "domain"
	ctxBizDefKey = "bizdef"
)

func GoliathInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	bizdef := ""
	if r, ok := req.(*pb.RetrieveRequest); ok {
		bizdef = r.BizDef

    		fmt.Println(r.String())
		domain, err := utils.ExtractDomain(r.Url)
		if err != nil && len(domain) > 0 {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		ctx = context.WithValue(ctx, ctxDomainKey, domain)
	}
	if r, ok := req.(*pb.SearchRequest); ok {
		bizdef = r.BizDef
	}
	if len(bizdef) == 0 {
		return nil, status.Error(codes.Unauthenticated, "No biz_def")
	}

	ctx = context.WithValue(ctx, ctxBizDefKey, bizdef)
	return handler(ctx, req)
}
