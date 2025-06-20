package executor

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "example.com/goliath/proto/goliath/v1"
	"example.com/goliath/internal/utils"
)

var (
	// Flags
	GreyRedirectRatio = flag.Int("grey_redirect_ratio", 0, "Traffic ration to rediret to grey env")

	// Context keys
	ctxReqIdKey = "reqid"
	ctxDomainKey = "domain"
	ctxBizDefKey = "bizdef"
	ctxDebugStrKey = "dbgstr"
)

func generateReqId() string {
	now := time.Now()
	return fmt.Sprintf("%04d%02d%02d%02d%02d%02d%06d", now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.UnixNano() % 1000000)
}

func RedirectToGrey(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// TODO(wanghaocheng) Warning this sucks!
	// Redirect part of traffic to goliath-portal-grey service
	// Need to be repleaced with more canonical approach
	conn, err := createGrpcConn("10.3.32.107:9023")

	if err != nil {
		return conn, err
	}
	defer conn.Close()

	if r, ok := req.(*pb.RetrieveRequest); ok {
		client := pb.NewGoliathPortalClient(conn)
		fres, ferr := client.Retrieve(ctx, r)
		if ferr != nil {
			glog.Error("Failed redirecting retrieve request: ", ferr)
		}
		return fres, ferr
	} else if r, ok := req.(*pb.SearchRequest); ok {
		client := pb.NewGoliathPortalClient(conn)
		fres, ferr := client.Search(ctx, r)
		if ferr != nil {
			glog.Error("Failed redirecting search requests: ", ferr)
		}
		return fres, ferr
	} else {
		glog.Error("Failed redirecting request, unknown method")
		return nil, fmt.Errorf("Unknown method")
	}
}

func GoliathInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	redirectToGrey := rand.Intn(100) < *GreyRedirectRatio
	if redirectToGrey {
		return RedirectToGrey(ctx, req, info, handler)
	}

	bizdef := ""
	reqId := ""
	startTime := time.Now()
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		glog.Info("Request ", reqId, " cost ", elapsedSeconds, " secs")
	}()

	if r, ok := req.(*pb.RetrieveRequest); ok {
		bizdef = fmt.Sprintf("%s_%s_%s", r.BizDef, r.SearchType, r.RetrieveType.String())
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

	// Debug string builder
	sb := &utils.StringBuilder{}
	sb.WriteString("[Method: Retrieve]")
	sb.WriteString(fmt.Sprintf(" [ReqId: %s]", reqId))
	ctx = context.WithValue(ctx, ctxDebugStrKey, sb)

	res, err := handler(ctx, req)
	if err != nil {
		return res, err
	}

	if r, ok := res.(*pb.RetrieveResponse); ok {
		r.RequestId = reqId
		r.DebugString = sb.String()
	}
	return res, nil
}
