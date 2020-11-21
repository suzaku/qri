package changes

import (
	"context"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qri/dsref"
)

type ChangeReport map[string]ComponentChange

type ChangeMeta struct {
	Title string `json:"title"`
}

type ChangeSummaryStats struct {
	Entries    int            `json:"entries"`         // "entries": 759166,
	Columns    int            `json:"columns"`         // "columns": 4,
	NullValues int            `json:"nullValues"`      // "nullValues": 0,
	TotalSize  int            `json:"totalSize"`       // "totalSize": 1047604210,
	Delta      map[string]int `json:"delta,omitempty"` // "delta": {
}

type ComponentChange struct {
	Meta  *ChangeMeta `json:"meta"`
	Left  interface{} `json:"left"`
	Right interface{} `json:"right"`

	Summary ComponentChange   `json:"summary"`
	Columns []ComponentChange `json:"columns"`
}

type Service struct {
	loader dsref.Loader
}

func NewService(loader dsref.Loader) *Service {
	return &Service{
		loader: loader,
	}
}

func (svc *Service) Report(ctx context.Context, leftRef, rightRef dsref.Ref, loadSource string) (ChangeReport, error) {
	left, err := svc.loader.LoadDataset(ctx, leftRef, loadSource)
	if err != nil {
		return nil, err
	}
	if rightRef.Path == "" && left.PreviousPath {
		rightRef.Path = left.PreviousPath
	}
	right, err := svc.loader.LoadDataset(ctx, rightRef, loadSource)
	if err != nil {
		return nil, err
	}

	report := ChangeReport{
		"commit": {
			Left:  left.Commit,
			Right: right.Commit,
		},
		"meta": {
			Left:  left.Meta,
			Right: right.Meta,
		},
		"stats": stats(left, right),
	}

	return report, nil
}

func stats(left, right *dataset.Dataset) ComponentChange {
	lStats := left.Stats.Stats.([]map[string]interface{})
	rStats := right.Stats.Stats.([]map[string]interface{})
	columns := make([]ComponentChange, len(lStats))
	for i := range columns {
		columns[i] = ComponentChange{
			Left:  lStats[i],
			Right: rStats[i],
		}
	}

	return ComponentChange{
		Summary: statsSummary(left, right),
		Columns: columns,
	}
}

func statsSummary(left, right *dataset.Dataset) ComponentChange {
	return ComponentChange{
		Left: ChangeSummaryStats{
			Entries:   left.Structure.Entries,
			TotalSize: right.Structure.Length,
		},
		Right: ChangeSummaryStats{
			Entries:   right.Structure.Entries,
			TotalSize: right.Structure.Length,
		},
	}
}
