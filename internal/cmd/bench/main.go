package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"google.golang.org/grpc"

	ydbSlog "github.com/ydb-platform/ydb-go-sdk-slog" // ← путь к адаптеру, который мы ранее писали
)

var (
	logger *slog.Logger
)

func init() {
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func main() {
	ctx := context.Background()

	var creds ydb.Option
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithAccessTokenCredentials(token)
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		creds = ydb.WithAnonymousCredentials()
	}

	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDialTimeout(5*time.Second),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydb.With(config.WithGrpcOptions(grpc.WithBlock())),
		ydbSlog.WithTraces(logger, trace.DiscoveryEvents|trace.DriverRepeaterEvents|trace.TableEvents|trace.DriverRepeaterEvents),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	wg := &sync.WaitGroup{}

	_ = upsertData(ctx, db.Table(), db.Name(), "series", 10)

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				start := time.Now()
				count, err := scanSelect(ctx, db.Table(), db.Name(), rand.Int63n(25000))
				logger.Debug("scan select",
					slog.Duration("latency", time.Since(start)),
					slog.Uint64("count", count),
					slog.Any("error", err),
				)
			}
		}()
	}
	wg.Wait()
}

func upsertData(ctx context.Context, c table.Client, prefix, tableName string, concurrency int) (err error) {
	_ = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.DropTable(ctx, path.Join(prefix, tableName))
	}, table.WithIdempotent())

	err = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, path.Join(prefix, tableName),
			options.WithColumn("series_id", types.Optional(types.TypeUint64)),
			options.WithColumn("title", types.Optional(types.TypeUTF8)),
			options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
			options.WithColumn("release_date", types.Optional(types.TypeUint64)),
			options.WithColumn("comment", types.Optional(types.TypeUTF8)),
			options.WithPrimaryKeyColumn("series_id"),
		)
	}, table.WithIdempotent())

	if err != nil {
		logger.Error("create table failed", slog.Any("error", err))
		return err
	}

	rowsLen := 25000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	sema := make(chan struct{}, concurrency)

	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.Uint64Value(uint64(time.Since(time.Unix(0, 0))/time.Hour/24))),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			err = c.Do(ctx, func(ctx context.Context, session table.Session) error {
				return session.BulkUpsert(ctx, path.Join(prefix, tableName), types.ListValue(rows...))
			}, table.WithIdempotent())

			if err == nil {
				logger.Debug("bulk upserted", slog.Int("from", shift), slog.Int("to", shift+batchSize))
			} else {
				logger.Error("bulk upsert failed", slog.Int("from", shift), slog.Int("to", shift+batchSize), slog.Any("error", err))
			}
		}(prefix, tableName, shift)
	}
	wg.Wait()
	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		$format = DateTime::Format("%%Y-%%m-%%d");
		SELECT
			series_id,
			title,
			$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)

	err = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		res, err := s.StreamExecuteScanQuery(ctx, query, table.NewQueryParameters())
		if err != nil {
			return err
		}
		var (
			id    *uint64
			title *string
			date  *[]byte
		)
		for res.NextResultSet(ctx, "series_id", "title", "release_date") {
			for res.NextRow() {
				count++
				err = res.Scan(&id, &title, &date)
				if err != nil {
					return err
				}
			}
		}
		return res.Err()
	}, table.WithIdempotent())

	return count, err
}
