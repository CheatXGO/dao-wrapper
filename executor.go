package dao_wrapper

import (
	"context"
	"errors"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Executor data access object
type Executor interface {
	RunInTransaction(ctx context.Context, f func(ctx context.Context) error) error
	NewQuery(ctx context.Context, impl interface{}) interface{}
}

type executor struct {
	*pgxpool.Pool
}

// NewExecutor creates connection pool accessor
func NewExecutor(conn *pgxpool.Pool) Executor {
	return &executor{conn}
}

// RunInTransaction runs function f inside db transaction block using specified executor
func (d *executor) RunInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	ctx, err := d.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = d.RollbackTransaction(ctx) }()

	if err = f(ctx); err != nil {
		return err
	}

	if err = d.CommitTransaction(ctx); err != nil {
		return err
	}

	return nil
}

// BeginTransaction start transaction runner and set it in context as txRunner key
func (d *executor) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := d.Begin(ctx)
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, "txRunner", tx), nil
}

// RollbackTransaction rollback tx
func (d *executor) RollbackTransaction(ctx context.Context) error {
	txRunner, ok := ctx.Value("txRunner").(pgx.Tx)
	if !ok {
		return errors.New("failed to rollback tx without runner")
	}

	return txRunner.Rollback(ctx)
}

// CommitTransaction commit tx
func (d *executor) CommitTransaction(ctx context.Context) error {
	txRunner, ok := ctx.Value("txRunner").(pgx.Tx)
	if !ok {
		return errors.New("failed to commit tx without runner")
	}

	return txRunner.Commit(ctx)
}

// NewQuery base query former for repository queries
// Example usage: declare type yourQuery struct {dao_wrapper.BaseQuery}
// then use NewQuery(ctx, &yourQuery{}).(*yourQuery) as constructor
func (d *executor) NewQuery(ctx context.Context, impl interface{}) interface{} {
	s := reflect.ValueOf(impl).Elem()
	baseQuery := BaseQuery{ctx: ctx, db: d.Pool}
	txRunner, ok := ctx.Value("txRunner").(pgx.Tx)
	if ok {
		baseQuery.tx = txRunner
	}
	s.FieldByName("BaseQuery").Set(reflect.ValueOf(baseQuery))
	return impl
}
