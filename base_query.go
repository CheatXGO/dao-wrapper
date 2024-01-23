package dao_wrapper

import (
	"context"

	"github.com/CheatXGO/scany/v2/pgxscan"
	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BaseQuery wrapper for queries
type BaseQuery struct {
	ctx context.Context
	db  *pgxpool.Pool
	tx  pgx.Tx
}

// Runner queries executor interface
type Runner interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
}

// SQLConverter query builder to sql with args converter (accept any squirrel builder interface)
type SQLConverter interface {
	ToSql() (string, []interface{}, error)
}

// PgQb sets placeholder format for postgres
func (q *BaseQuery) PgQb() squirrel.StatementBuilderType {
	return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
}

// Context returns context of the query
func (q *BaseQuery) Context() context.Context {
	return q.ctx
}

// Runner returns runner for operations with the database
func (q *BaseQuery) Runner() Runner {
	if q.tx != nil {
		return q.tx
	}

	return q.db
}

// Get query for only one row. If no rows are found it returns a pgx.ErrNoRows error.
func (q *BaseQuery) Get(sq SQLConverter, resp interface{}) error {
	sql, args, _ := sq.ToSql()
	err := pgxscan.Get(q.Context(), q.Runner(), resp, sql, args...)
	if err != nil {
		return err
	}
	return nil
}

// Select query for many rows. Accept slice as destination resp. If no rows are found - it returns nil error.
func (q *BaseQuery) Select(sq SQLConverter, resp interface{}) error {
	sql, args, _ := sq.ToSql()
	err := pgxscan.Select(q.Context(), q.Runner(), resp, sql, args...)
	if err != nil {
		return err
	}
	return nil
}

// Exec query for no result queries (insert/update/delete without "RETURNING any" suffix)
func (q *BaseQuery) Exec(sq SQLConverter) error {
	sql, args, _ := sq.ToSql()
	_, err := q.Runner().Exec(q.Context(), sql, args...)
	if err != nil {
		return err
	}
	return nil
}
