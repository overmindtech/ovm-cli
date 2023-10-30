package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/overmindtech/ovm-cli/tracing"
	"github.com/overmindtech/sdp-go"
	"github.com/overmindtech/sdp-go/sdpws"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// requestCmd represents the start command
var requestCmd = &cobra.Command{
	Use:   "request",
	Short: "Runs a request against the overmind API",
	PreRun: func(cmd *cobra.Command, args []string) {
		// Bind these to viper
		err := viper.BindPFlags(cmd.Flags())
		if err != nil {
			log.WithError(err).Fatal("could not bind `request` flags")
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		sigs := make(chan os.Signal, 1)

		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a goroutine to watch for cancellation signals
		go func() {
			select {
			case <-sigs:
				cancel()
			case <-ctx.Done():
			}
		}()

		exitcode := Request(ctx, nil)
		tracing.ShutdownTracer()
		os.Exit(exitcode)
	},
}

func Request(ctx context.Context, ready chan bool) int {
	timeout, err := time.ParseDuration(viper.GetString("timeout"))
	if err != nil {
		log.Errorf("invalid --timeout value '%v', error: %v", viper.GetString("timeout"), err)
		return 1
	}
	ctx, span := tracing.Tracer().Start(ctx, "CLI Request", trace.WithAttributes(
		attribute.String("om.config", fmt.Sprintf("%v", viper.AllSettings())),
	))
	defer span.End()

	// Construct the request
	req, err := createInitialRequest()
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to create initial request")
		return 1
	}

	gatewayUrl := viper.GetString("gateway-url")
	if gatewayUrl == "" {
		gatewayUrl = fmt.Sprintf("%v/api/gateway", viper.GetString("url"))
		viper.Set("gateway-url", gatewayUrl)
	}

	lf := log.Fields{}

	ctx, err = ensureToken(ctx, []string{"explore:read"})
	if err != nil {
		log.WithContext(ctx).WithFields(lf).WithField("api-key-url", viper.GetString("api-key-url")).WithError(err).Error("failed to authenticate")
		return 1
	}

	// apply a timeout to the main body of processing
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	mgmtClient := AuthenticatedManagementClient(ctx)
	log.WithContext(ctx).WithFields(lf).Info("Waking up sources")
	_, err = mgmtClient.KeepaliveSources(ctx, &connect.Request[sdp.KeepaliveSourcesRequest]{
		Msg: &sdp.KeepaliveSourcesRequest{
			WaitForHealthy: true,
		},
	})
	if err != nil {
		log.WithContext(ctx).WithFields(lf).WithError(err).Error("Failed to wake up sources")
		return 1
	}

	c, err := sdpws.Dial(ctx, gatewayUrl,
		NewAuthenticatedClient(ctx, otelhttp.DefaultClient),
		&sdpws.LoggingGatewayMessageHandler{Level: log.InfoLevel},
	)
	if err != nil {
		lf["gateway-url"] = gatewayUrl
		log.WithContext(ctx).WithFields(lf).WithError(err).Error("Failed to connect to overmind API")
		return 1
	}
	defer c.Close(ctx)

	// Log the request in JSON
	b, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		log.WithContext(ctx).WithFields(lf).WithError(err).Error("Failed to marshal request")
		return 1
	}

	log.WithContext(ctx).WithFields(lf).Infof("Request:\n%v", string(b))
	q, err := createQuery()
	if err != nil {
		log.WithContext(ctx).WithFields(lf).WithError(err).Error("Failed to create query")
		return 1
	}
	err = c.SendQuery(ctx, q)
	if err != nil {
		log.WithContext(ctx).WithFields(lf).WithError(err).Error("Failed to execute query")
		return 1
	}
	log.WithContext(ctx).WithFields(lf).WithError(err).Info("received items")

	c.Wait(ctx, uuid.UUIDs{uuid.UUID(q.UUID)})

	// 	queriesSent := true

	// 	responses := make(chan *sdp.GatewayResponse)

	// 	// Start a goroutine that reads responses
	// 	go func() {
	// 		for {
	// 			res := new(sdp.GatewayResponse)

	// 			err = wspb.Read(ctx, c, res)

	// 			if err != nil {
	// 				var e websocket.CloseError
	// 				if errors.As(err, &e) {
	// 					log.WithContext(ctx).WithFields(log.Fields{
	// 						"code":   e.Code.String(),
	// 						"reason": e.Reason,
	// 					}).Info("Websocket closing")
	// 					return
	// 				}
	// 				log.WithContext(ctx).WithFields(log.Fields{
	// 					"error": err,
	// 				}).Error("Failed to read response")
	// 				return
	// 			}

	// 			responses <- res
	// 		}
	// 	}()

	// 	activeQueries := make(map[uuid.UUID]bool)

	// 	var numItems, numEdges int

	// 	// Read the responses
	// responses:
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			log.WithContext(ctx).WithFields(lf).Info("Context cancelled, exiting")
	// 			return 1

	// 		case resp := <-responses:
	// 			switch resp.ResponseType.(type) {
	// 			case *sdp.GatewayResponse_Status:
	// 				status := resp.GetStatus()
	// 				statusFields := log.Fields{
	// 					"summary":                status.Summary,
	// 					"responders":             status.Summary.Responders,
	// 					"queriesSent":            queriesSent,
	// 					"postProcessingComplete": status.PostProcessingComplete,
	// 					"itemsReceived":          numItems,
	// 					"edgesReceived":          numEdges,
	// 				}

	// 				if status.Done() {
	// 					// fall through from all "final" query states, check if there's still queries in progress;
	// 					// only break from the loop if all queries have already been sent
	// 					// TODO: see above, still needs DefaultStartTimeout implemented to account for slow sources
	// 					allDone := allDone(ctx, activeQueries, lf)
	// 					statusFields["allDone"] = allDone
	// 					if allDone && queriesSent {
	// 						log.WithContext(ctx).WithFields(lf).WithFields(statusFields).Info("all responders and queries done")
	// 						break responses
	// 					} else {
	// 						log.WithContext(ctx).WithFields(lf).WithFields(statusFields).Info("all responders done, with unfinished queries")
	// 					}
	// 				} else {
	// 					log.WithContext(ctx).WithFields(lf).WithFields(statusFields).Info("still waiting for responders")
	// 				}

	// 			case *sdp.GatewayResponse_QueryStatus:
	// 				status := resp.GetQueryStatus()
	// 				statusFields := log.Fields{
	// 					"status": status.Status.String(),
	// 				}
	// 				queryUuid := status.GetUUIDParsed()
	// 				if queryUuid == nil {
	// 					log.WithContext(ctx).WithFields(lf).WithFields(statusFields).Debugf("Received QueryStatus with nil UUID")
	// 					continue responses
	// 				}
	// 				statusFields["query"] = queryUuid

	// 				switch status.Status {
	// 				case sdp.QueryStatus_UNSPECIFIED:
	// 					statusFields["unexpected_status"] = true
	// 				case sdp.QueryStatus_STARTED:
	// 					activeQueries[*queryUuid] = true
	// 				case sdp.QueryStatus_FINISHED:
	// 					activeQueries[*queryUuid] = false
	// 				case sdp.QueryStatus_ERRORED:
	// 					activeQueries[*queryUuid] = false
	// 				case sdp.QueryStatus_CANCELLED:
	// 					activeQueries[*queryUuid] = false
	// 				default:
	// 					statusFields["unexpected_status"] = true
	// 				}

	// 				log.WithContext(ctx).WithFields(lf).WithFields(statusFields).Debugf("query status update")

	// 			case *sdp.GatewayResponse_NewItem:
	// 				item := resp.GetNewItem()
	// 				numItems += 1
	// 				log.WithContext(ctx).WithFields(lf).WithField("item", item.GloballyUniqueName()).Infof("new item")

	// 			case *sdp.GatewayResponse_NewEdge:
	// 				edge := resp.GetNewEdge()
	// 				numEdges += 1
	// 				log.WithContext(ctx).WithFields(lf).WithFields(log.Fields{
	// 					"from": edge.From.GloballyUniqueName(),
	// 					"to":   edge.To.GloballyUniqueName(),
	// 				}).Info("new edge")

	// 			case *sdp.GatewayResponse_QueryError:
	// 				err := resp.GetQueryError()
	// 				log.WithContext(ctx).WithFields(lf).Errorf("Error from %v(%v): %v", err.ResponderName, err.SourceName, err)

	// 			case *sdp.GatewayResponse_Error:
	// 				err := resp.GetError()
	// 				log.WithContext(ctx).WithFields(lf).Errorf("generic error: %v", err)

	// 			default:
	// 				j := protojson.Format(resp)
	// 				log.WithContext(ctx).WithFields(lf).Infof("Unknown %T Response:\n%v", resp.ResponseType, j)
	// 			}
	// 		}
	// 	}

	// 	if viper.GetBool("snapshot-after") {
	// 		log.WithContext(ctx).Info("Starting snapshot")
	// 		msgId := uuid.New()
	// 		snapReq := &sdp.GatewayRequest{
	// 			MinStatusInterval: minStatusInterval,
	// 			RequestType: &sdp.GatewayRequest_StoreSnapshot{
	// 				StoreSnapshot: &sdp.StoreSnapshot{
	// 					Name:        viper.GetString("snapshot-name"),
	// 					Description: viper.GetString("snapshot-description"),
	// 					MsgID:       msgId[:],
	// 				},
	// 			},
	// 		}
	// 		err = wspb.Write(ctx, c, snapReq)
	// 		if err != nil {
	// 			log.WithContext(ctx).WithFields(log.Fields{
	// 				"error": err,
	// 			}).Error("Failed to send snapshot request")
	// 			return 1
	// 		}

	// 		for {
	// 			select {
	// 			case <-ctx.Done():
	// 				log.WithContext(ctx).Info("Context cancelled, exiting")
	// 				return 1
	// 			case resp := <-responses:
	// 				switch resp.ResponseType.(type) {
	// 				case *sdp.GatewayResponse_SnapshotStoreResult:
	// 					result := resp.GetSnapshotStoreResult()
	// 					if result.Success {
	// 						log.WithContext(ctx).Infof("Snapshot stored successfully: %v", uuid.UUID(result.SnapshotID))
	// 						return 0
	// 					}

	// 					log.WithContext(ctx).Errorf("Snapshot store failed: %v", result.ErrorMessage)
	// 					return 1
	// 				default:
	// 					j := protojson.Format(resp)

	// 					log.WithContext(ctx).Infof("Unknown %T Response:\n%v", resp.ResponseType, j)
	// 				}
	// 			}
	// 		}
	// 	}
	return 0
}

func methodFromString(method string) (sdp.QueryMethod, error) {
	var result sdp.QueryMethod

	switch method {
	case "get":
		result = sdp.QueryMethod_GET
	case "list":
		result = sdp.QueryMethod_LIST
	case "search":
		result = sdp.QueryMethod_SEARCH
	default:
		return 0, fmt.Errorf("query method '%v' not supported", method)
	}
	return result, nil
}

func createQuery() (*sdp.Query, error) {
	u := uuid.New()
	method, err := methodFromString(viper.GetString("query-method"))
	if err != nil {
		return nil, err
	}

	return &sdp.Query{
		Method:   method,
		Type:     viper.GetString("query-type"),
		Query:    viper.GetString("query"),
		Scope:    viper.GetString("query-scope"),
		Deadline: timestamppb.New(time.Now().Add(10 * time.Hour)),
		UUID:     u[:],
		RecursionBehaviour: &sdp.Query_RecursionBehaviour{
			LinkDepth:                  viper.GetUint32("link-depth"),
			FollowOnlyBlastPropagation: viper.GetBool("blast-radius"),
		},
		IgnoreCache: viper.GetBool("ignore-cache"),
	}, nil
}

func createInitialRequest() (*sdp.GatewayRequest, error) {
	req := &sdp.GatewayRequest{
		MinStatusInterval: minStatusInterval,
	}

	switch viper.GetString("request-type") {
	case "query":
		q, err := createQuery()
		if err != nil {
			return nil, err
		}

		req.RequestType = &sdp.GatewayRequest_Query{
			Query: q,
		}
	case "load-bookmark":
		bookmarkUUID, err := uuid.Parse(viper.GetString("bookmark-uuid"))
		if err != nil {
			return nil, err
		}
		msgID := uuid.New()
		req.RequestType = &sdp.GatewayRequest_LoadBookmark{
			LoadBookmark: &sdp.LoadBookmark{
				UUID:        bookmarkUUID[:],
				MsgID:       msgID[:],
				IgnoreCache: viper.GetBool("ignore-cache"),
			},
		}
	case "load-snapshot":
		snapshotUUID, err := uuid.Parse(viper.GetString("snapshot-uuid"))
		if err != nil {
			return nil, err
		}
		msgID := uuid.New()
		req.RequestType = &sdp.GatewayRequest_LoadSnapshot{
			LoadSnapshot: &sdp.LoadSnapshot{
				UUID:  snapshotUUID[:],
				MsgID: msgID[:],
			},
		}
	default:
		return nil, fmt.Errorf("request type %v not supported", viper.GetString("request-type"))
	}

	return req, nil
}

func init() {
	rootCmd.AddCommand(requestCmd)

	requestCmd.PersistentFlags().String("request-type", "query", "The type of request to send (query, load-bookmark, load-snapshot)")

	requestCmd.PersistentFlags().String("query-method", "get", "The method to use (get, list, search)")
	requestCmd.PersistentFlags().String("query-type", "*", "The type to query")
	requestCmd.PersistentFlags().String("query", "", "The actual query to send")
	requestCmd.PersistentFlags().String("query-scope", "*", "The scope to query")
	requestCmd.PersistentFlags().Bool("ignore-cache", false, "Set to true to ignore all caches in overmind.")

	requestCmd.PersistentFlags().String("bookmark-uuid", "", "The UUID of the bookmark to load")
	requestCmd.PersistentFlags().String("snapshot-uuid", "", "The UUID of the snapshot to load")

	requestCmd.PersistentFlags().Bool("snapshot-after", false, "Set this to create a snapshot of the query results")
	requestCmd.PersistentFlags().String("snapshot-name", "CLI", "The snapshot name of the query results")
	requestCmd.PersistentFlags().String("snapshot-description", "none", "The snapshot description of the query results")

	requestCmd.PersistentFlags().String("timeout", "5m", "How long to wait for responses")
	requestCmd.PersistentFlags().Uint32("link-depth", 0, "How deeply to link")
	requestCmd.PersistentFlags().Bool("blast-radius", false, "Whether to query using blast radius, note that if using this option, link-depth should be set to > 0")
}
