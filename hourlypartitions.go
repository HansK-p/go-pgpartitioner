package pgpartitioner

import (
	"time"
)

type HourlyPartitionOptions struct {
	TableName     string
	HoursForward  int
	HoursBackward int
}

func (hourlyPartitionOptions *HourlyPartitionOptions) boundary(baseTime time.Time) (boundaryType time.Time) {
	return time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), baseTime.Hour(), 0, 0, 0, baseTime.Location())
}

func (hourlyPartitionOptions *HourlyPartitionOptions) BoundarySqlText(boundaryTime time.Time) (boundarySqlTxt string) {
	return boundaryTime.Format("'2006-01-02 15:00:00'")
}

func (hourlyPartitionOptions *HourlyPartitionOptions) Table() (table string) {
	return hourlyPartitionOptions.TableName
}

func (hourlyPartitionOptions *HourlyPartitionOptions) PartitionTable(curTime time.Time) (partitionTable string) {
	return hourlyPartitionOptions.TableName + "_" + curTime.Format(`20060102H15`)
}

func (hourlyPartitionOptions *HourlyPartitionOptions) PrevBoundary(curTime time.Time) (boundaryTime time.Time) {
	return hourlyPartitionOptions.boundary(curTime.Add(-time.Nanosecond))
}

func (hourlyPartitionOptions *HourlyPartitionOptions) NextBoundary(curTime time.Time) (boundaryTime time.Time) {
	return hourlyPartitionOptions.boundary(curTime.Add(time.Hour))
}

func (hourlyPartitionOptions *HourlyPartitionOptions) NotBeforeBoundary() (boundaryTime time.Time) {
	return hourlyPartitionOptions.boundary(time.Now().Add(-time.Duration(hourlyPartitionOptions.HoursBackward) * time.Hour))
}
func (hourlyPartitionOptions *HourlyPartitionOptions) NotAfterBoundary() (boundaryTime time.Time) {
	return hourlyPartitionOptions.boundary(time.Now().Add(time.Duration(hourlyPartitionOptions.HoursForward+1)*time.Hour - time.Nanosecond))
}
