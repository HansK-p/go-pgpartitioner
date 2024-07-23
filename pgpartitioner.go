package pgpartitioner

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgPartitionsCreateOptions interface {
	Table() (table string)
	BoundarySqlText(boundaryTime time.Time) (boundarySqlTxt string)
	PartitionTable(curTime time.Time) (partitionName string)
	PrevBoundary(curTime time.Time) (boundaryTime time.Time)
	NextBoundary(curTime time.Time) (boundaryTime time.Time)
	NotBeforeBoundary() (boundaryTime time.Time)
	NotAfterBoundary() (boundaryTime time.Time)
}

type PgPartitionsDeleteOptions interface {
	PartitionTable(curTime time.Time) (partitionName string)
	PrevBoundary(curTime time.Time) (boundaryTime time.Time)
	NotBeforeBoundary() (boundaryTime time.Time)
}

func tableExists(tx *sql.Tx, table string) (exists bool, err error) {
	partitionExists := 0
	if err := tx.QueryRow("SELECT count(1) FROM information_schema.tables where lower(table_name) = lower($1)", table).Scan(&partitionExists); err != nil {
		return false, fmt.Errorf("when checking if the table '%s' already exists: %w", table, err)
	}
	return partitionExists != 0, nil
}

func PartitionCreate(tx *sql.Tx, options PgPartitionsCreateOptions) (partitionTablesAdded []string, err error) {
	partitionTablesToAdd := []string{}
	partitionTablesTime := []time.Time{}

	for partitionTime := options.NotAfterBoundary(); partitionTime.After(options.NotBeforeBoundary()); {
		partitionTime = options.PrevBoundary(partitionTime)
		partitionTable := options.PartitionTable(partitionTime)
		if exists, err := tableExists(tx, partitionTable); err != nil {
			return nil, fmt.Errorf("when checking if partition table already exists")
		} else if !exists {
			partitionTablesToAdd = append(partitionTablesToAdd, partitionTable)
			partitionTablesTime = append(partitionTablesTime, partitionTime)
		}
	}

	for idx := len(partitionTablesToAdd) - 1; idx >= 0; idx-- {
		sql := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)",
			partitionTablesToAdd[idx], options.Table(),
			options.BoundarySqlText(partitionTablesTime[idx]), options.BoundarySqlText(options.NextBoundary(partitionTablesTime[idx])),
		)
		if _, err := tx.Exec(sql); err != nil {
			return partitionTablesAdded, fmt.Errorf("when creating partition table '%s' using sql '%s': %w", partitionTablesToAdd[idx], sql, err)
		}
		partitionTablesAdded = append(partitionTablesAdded, partitionTablesToAdd[idx])
	}
	return partitionTablesAdded, nil
}

func PartitionCleanup(tx *sql.Tx, options PgPartitionsDeleteOptions) (partitionTablesDeleted []string, err error) {
	partitionTablesToDelete := []string{}
	for partitionTime := options.NotBeforeBoundary(); ; {
		partitionTime = options.PrevBoundary(partitionTime)
		partitionTable := options.PartitionTable(partitionTime)
		if exists, err := tableExists(tx, partitionTable); err != nil {
			return nil, fmt.Errorf("when checking if the partition table already exists: %w", err)
		} else if !exists {
			break
		}
		partitionTablesToDelete = append(partitionTablesToDelete, partitionTable)
	}
	for idx := len(partitionTablesToDelete) - 1; idx >= 0; idx-- {
		if _, err := tx.Exec(fmt.Sprintf(`drop table %s`, partitionTablesToDelete[idx])); err != nil {
			return partitionTablesDeleted, fmt.Errorf("when deleting partition table '%s': %w", partitionTablesToDelete[idx], err)
		}
		partitionTablesDeleted = append(partitionTablesDeleted, partitionTablesToDelete[idx])
	}
	return
}
