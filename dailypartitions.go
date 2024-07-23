package pgpartitioner

import (
	"time"
)

type DailyPartitionOptions struct {
	DaysForward  int
	DaysBackward int
}

func (dailyPartitionOptions *DailyPartitionOptions) boundary(baseTime time.Time) (boundaryType time.Time) {
	return time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 0, 0, 0, 0, baseTime.Location())
}

func (dailyPartitionOptions *DailyPartitionOptions) BoundarySqlText(boundaryTime time.Time) (boundarySqlTxt string) {
	return boundaryTime.Format("'2006-01-02'")
}

func (dailyPartitionOptions *DailyPartitionOptions) Table() (table string) { return "iislogentry" }

func (dailyPartitionOptions *DailyPartitionOptions) PartitionTable(curTime time.Time) (partitionTable string) {
	return "iislogentry_" + curTime.Format(`20060102`)
}

func (dailyPartitionOptions *DailyPartitionOptions) PrevBoundary(curTime time.Time) (boundaryTime time.Time) {
	return dailyPartitionOptions.boundary(curTime.Add(-time.Nanosecond))
}

func (dailyPartitionOptions *DailyPartitionOptions) NextBoundary(curTime time.Time) (boundaryTime time.Time) {
	return dailyPartitionOptions.boundary(curTime.AddDate(0, 0, 1))
}

func (dailyPartitionOptions *DailyPartitionOptions) NotBeforeBoundary() (boundaryTime time.Time) {
	return dailyPartitionOptions.boundary(time.Now().AddDate(0, 0, -dailyPartitionOptions.DaysBackward))
}
func (dailyPartitionOptions *DailyPartitionOptions) NotAfterBoundary() (boundaryTime time.Time) {
	return dailyPartitionOptions.boundary(time.Now().AddDate(0, 0, dailyPartitionOptions.DaysForward))
}
