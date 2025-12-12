package orm

import (
	"database/sql"
	"github.com/banbox/banbot/orm/migrations"
	"github.com/banbox/banexg/log"
	"github.com/pressly/goose/v3"
	"go.uber.org/zap"
)

// Migrate runs the database migrations using goose
func Migrate(db *sql.DB, dialect string) error {
	// Set the file system for migrations
	goose.SetBaseFS(migrations.EmbedMigrations)

	// Set the dialect (postgres or sqlite3)
	if err := goose.SetDialect(dialect); err != nil {
		log.Error("failed to set dialect", zap.String("dialect", dialect), zap.Error(err))
		return err
	}

	// Run migrations
	// Use "no-versioning" if you don't want the version table, but here we want it.
	// We point to "." because EmbedMigrations embeds the root of package which matches "." in FS.
	if err := goose.Up(db, "."); err != nil {
		log.Error("goose up failed", zap.Error(err))
		return err
	}

	log.Info("migrations completed successfully", zap.String("dialect", dialect))
	return nil
}
