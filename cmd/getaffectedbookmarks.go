package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/overmindtech/ovm-cli/tracing"
	"github.com/overmindtech/sdp-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// getAffectedBookmarksCmd represents the get-affected-bookmarks command
var getAffectedBookmarksCmd = &cobra.Command{
	Use:   "get-affected-bookmarks --snapshot-uuid ID --bookmark-uuids ID,ID,ID",
	Short: "Calculates the bookmarks that would be overlapping with a snapshot.",
	PreRun: func(cmd *cobra.Command, args []string) {
		// Bind these to viper
		err := viper.BindPFlags(cmd.Flags())
		if err != nil {
			log.WithError(err).Fatal("could not bind `get-affected-bookmarks` flags")
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

		exitcode := GetAffectedBookmarks(ctx, nil)
		tracing.ShutdownTracer()
		os.Exit(exitcode)
	},
}

func GetAffectedBookmarks(ctx context.Context, ready chan bool) int {
	timeout, err := time.ParseDuration(viper.GetString("timeout"))
	if err != nil {
		log.Errorf("invalid --timeout value '%v', error: %v", viper.GetString("timeout"), err)
		return 1
	}

	snapshotUuid, err := uuid.Parse(viper.GetString("snapshot-uuid"))
	if err != nil {
		log.Errorf("invalid --snapshot-uuid value '%v', error: %v", viper.GetString("uuid"), err)
		return 1
	}

	uuidStrings := viper.GetStringSlice("bookmark-uuids")
	bookmarkUuids := [][]byte{}
	for _, s := range uuidStrings {
		bookmarkUuid, err := uuid.Parse(s)
		if err != nil {
			log.Errorf("invalid --bookmark-uuids value '%v', error: %v", bookmarkUuid, err)
			return 1
		}
		bookmarkUuids = append(bookmarkUuids, bookmarkUuid[:])
	}

	ctx, span := tracing.Tracer().Start(ctx, "CLI GetAffectedBookmarks", trace.WithAttributes(
		attribute.String("ovm.config", fmt.Sprintf("%v", viper.AllSettings())),
	))
	defer span.End()

	ctx, err = ensureToken(ctx, []string{"changes:read"})
	if err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"url": viper.GetString("url"),
		}).Error("failed to authenticate")
		return 1
	}

	// apply a timeout to the main body of processing
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := AuthenticatedBookmarkClient(ctx)
	response, err := client.GetAffectedBookmarks(ctx, &connect.Request[sdp.GetAffectedBookmarksRequest]{
		Msg: &sdp.GetAffectedBookmarksRequest{
			SnapshotUUID:  snapshotUuid[:],
			BookmarkUUIDs: bookmarkUuids,
		},
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"bookmark-url": viper.GetString("bookmark-url"),
		}).Error("failed to get affected bookmarks")
		return 1
	}
	for _, u := range response.Msg.BookmarkUUIDs {
		bookmarkUuid := uuid.UUID(u)
		log.WithContext(ctx).WithFields(log.Fields{
			"uuid": bookmarkUuid,
		}).Info("found affected bookmark")
	}
	return 0
}

func init() {
	rootCmd.AddCommand(getAffectedBookmarksCmd)

	getAffectedBookmarksCmd.PersistentFlags().String("bookmark-url", "", "The bookmark service API endpoint (defaults to --url)")
	getAffectedBookmarksCmd.PersistentFlags().String("frontend", "https://app.overmind.tech/", "The frontend base URL")

	getAffectedBookmarksCmd.PersistentFlags().String("snapshot-uuid", "", "The UUID of the snapshot that should be checked.")
	getAffectedBookmarksCmd.PersistentFlags().String("bookmark-uuids", "", "A comma separated list of UUIDs of the potentially affected bookmarks.")

	getAffectedBookmarksCmd.PersistentFlags().String("timeout", "5m", "How long to wait for responses")
}
