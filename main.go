package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.coldcutz.net/go-stuff/utils"
	"golang.org/x/sync/errgroup"

	"go.coldcutz.net/crdbtestjobs/models"
)

//go:generate go run github.com/sqlc-dev/sqlc/cmd/sqlc@latest generate

// embed
//
//go:embed schema.sql
var schemaSql string

func main() {
	ctx, done, log, err := utils.StdSetup()
	if err != nil {
		panic(err)
	}
	defer done()

	if err := run(ctx, log); err != nil {
		log.Error("run error", "err", err.Error())
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	numWorkers, err := strconv.Atoi(os.Getenv("NUM_WORKERS"))
	if err != nil {
		return fmt.Errorf("strconv.Atoi(NUM_WORKERS): %w", err)
	}

	config, err := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("pgx.ParseConfig: %w", err)
	}
	config.RuntimeParams["application_name"] = "cc/crdbtestjobs"

	mConn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("pgx.ConnectConfig: %w", err)
	}
	defer mConn.Close(ctx)

	log.Debug("running schema.sql")
	err = crdbpgx.ExecuteTx(ctx, mConn, pgx.TxOptions{AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, schemaSql)
		return err
	})
	if err != nil {
		return fmt.Errorf("crdbpgx.ExecuteTx (schema.sql): %w", err)
	}

	done := make(chan struct{})

	queries := models.New()

	log.Debug("resetting jobs")
	err = crdbpgx.ExecuteTx(ctx, mConn, pgx.TxOptions{AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
		if err := queries.WipeJobs(ctx, tx); err != nil {
			return fmt.Errorf("queries.WipeJobs: %w", err)
		}

		numJobs := &pgtype.Numeric{}
		if err := numJobs.ScanInt64(pgtype.Int8{Int64: 100, Valid: true}); err != nil {
			return fmt.Errorf("numJobs.ScanInt64: %w", err)
		}
		if err := queries.SeedJobs(ctx, tx, *numJobs); err != nil {
			return fmt.Errorf("queries.SeedJobs: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("crdbpgx.ExecuteTx (reset jobs): %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	// monitor loop
	eg.Go(func() error {
		conn, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			return fmt.Errorf("pgx.ConnectConfig: %w", err)
		}
		defer conn.Close(ctx)

		if iso := os.Getenv("ISOLATION_LEVEL"); iso != "" {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", iso)); err != nil {
				return fmt.Errorf("setting isolation level: %w", err)
			}
		}
		if os.Getenv("SET_STUFF") != "" {
			if _, err := conn.Exec(ctx, "SET optimizer_use_lock_op_for_serializable = on"); err != nil {
				return fmt.Errorf("setting optimizer_use_lock_op_for_serializable: %w", err)
			}
			if _, err := conn.Exec(ctx, "SET enable_durable_locking_for_serializable = true"); err != nil {
				return fmt.Errorf("setting enable_durable_locking_for_serializable: %w", err)
			}
		}

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}

			noneLeft := false
			err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
				js, err := queries.ListPendingJobs(ctx, tx)
				if err != nil {
					return fmt.Errorf("queries.ListPendingJobs: %w", err)
				}
				log.Info("num pending jobs", "num", len(js))
				if len(js) == 0 {
					log.Info("no pending jobs")
					noneLeft = true
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("crdbpgx.ExecuteTx: %w", err)
			}
			if noneLeft {
				close(done)
				return nil
			}
		}
		return nil
	})

	for wi := 0; wi < numWorkers; wi++ {
		eg.Go(func() error {
			log := log.With("worker", wi)
			defer log.Info("worker done", "wi", wi)
			conn, err := pgx.ConnectConfig(ctx, config)
			if err != nil {
				return fmt.Errorf("pgx.ConnectConfig: %w", err)
			}
			defer conn.Close(ctx)

			// set session vars
			if iso := os.Getenv("ISOLATION_LEVEL"); iso != "" {
				if _, err := conn.Exec(ctx, fmt.Sprintf("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", iso)); err != nil {
					return fmt.Errorf("setting isolation level: %w", err)
				}
			}
			if os.Getenv("SET_STUFF") != "" {
				if _, err := conn.Exec(ctx, "SET optimizer_use_lock_op_for_serializable = on"); err != nil {
					return fmt.Errorf("setting optimizer_use_lock_op_for_serializable: %w", err)
				}
				if _, err := conn.Exec(ctx, "SET enable_durable_locking_for_serializable = true"); err != nil {
					return fmt.Errorf("setting enable_durable_locking_for_serializable: %w", err)
				}
			}

			if _, err := conn.Exec(ctx, "SET transaction_timeout = '30s'"); err != nil {
				return fmt.Errorf("setting lock_timeout: %w", err)
			}

			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				default:
				}

				err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
					job, err := queries.GetJob(ctx, tx)
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							log.Info("no job found")

							// wait a bit
							select {
							case <-time.After(1 * time.Second):
							case <-ctx.Done():
								return ctx.Err()
							}

							return nil
						}
						return fmt.Errorf("queries.GetJob: %w", err)
					}

					log.Info("job found", "job", job.Name)

					// do the job
					select {
					case <-time.After(5*time.Second + time.Duration(rand.Intn(100))*time.Millisecond):
					case <-ctx.Done():
						return ctx.Err()
					}

					job, err = queries.FinishJob(ctx, tx, models.FinishJobParams{
						ID:          job.ID,
						CompletedBy: pgtype.Text{Valid: true, String: fmt.Sprintf("cc/crdbtestjobs %d", wi)},
					})
					if err != nil {
						return fmt.Errorf("queries.FinishJob: %w", err)
					}

					log.Info("job finished", "job", job)

					return nil
				})
				if err != nil {
					return fmt.Errorf("crdbpgx.ExecuteTx: %w", err)
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("errgroup.Wait: %w", err)
	}

	return nil
}
