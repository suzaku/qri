package changes

import (
	"context"
	"encoding/json"
	"fmt"

	golog "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/tabular"
	"github.com/qri-io/qri/dsref"
	"github.com/qri-io/qri/fsi"
	"github.com/qri-io/qri/stats"
)

var log = golog.Logger("changereport")

// ChangeReportComponent is a generic component used to populate the change report
type ChangeReportComponent struct {
	Left  interface{}            `json:"left"`
	Right interface{}            `json:"right"`
	Meta  map[string]interface{} `json:"meta"`
}

// ChangeReportDeltaComponent is a subcomponent that can hold
// delta information between left and right
type ChangeReportDeltaComponent struct {
	Left  interface{}            `json:"left"`
	Right interface{}            `json:"right"`
	Title string                 `json:"title,omitempty"`
	Delta interface{}            `json:"delta"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
}

// StatsChangeComponent represents the stats change report
type StatsChangeComponent struct {
	Summary *ChangeReportDeltaComponent   `json:"summary"`
	Columns []*ChangeReportDeltaComponent `json:"columns"`
}

// ChangeReportResponse is the result of a call to changereport
type ChangeReportResponse struct {
	VersionInfo *ChangeReportComponent `json:"version_info,omitempty"`
	Commit      *ChangeReportComponent `json:"commit,omitempty"`
	Meta        *ChangeReportComponent `json:"meta,omitempty"`
	Readme      *ChangeReportComponent `json:"readme,omitempty"`
	Structure   *ChangeReportComponent `json:"structure,omitempty"`
	Transform   *ChangeReportComponent `json:"transform,omitempty"`
	Stats       *StatsChangeComponent  `json:"stats,omitempty"`
}

// StatsChangeSummaryFields represents the stats summary
type StatsChangeSummaryFields struct {
	Entries int `json:"entries"`
	Columns int `json:"columns"`
	// NullValues int `json:"nullValues"`
	TotalSize int `json:"totalSize"`
}

// EmptyObject is used mostly as a placeholder in palces where it is required
// that a key is present in the response even if empty and not be nil
type EmptyObject map[string]interface{}

// Service can generate a change report between two datasets
type Service struct {
	loader dsref.Loader
	stats  *stats.Service
}

// New allocates a Change service
func New(loader dsref.Loader, stats *stats.Service) *Service {
	return &Service{
		loader: loader,
		stats:  stats,
	}
}

func (svc *Service) statsDiff(ctx context.Context, leftDs *dataset.Dataset, rightDs *dataset.Dataset) (*StatsChangeComponent, error) {
	res := &StatsChangeComponent{}

	res.Summary = &ChangeReportDeltaComponent{}
	leftColCount := 0
	rightColCount := 0
	var leftColItems, rightColItems tabular.Columns
	var sErr error
	if leftDs.Structure != nil {
		leftColItems, _, sErr = tabular.ColumnsFromJSONSchema(leftDs.Structure.Schema)
		if sErr != nil {
			return &StatsChangeComponent{}, sErr
		}
		leftColCount = len(leftColItems)

		res.Summary.Left = StatsChangeSummaryFields{
			Entries:   leftDs.Structure.Entries,
			Columns:   leftColCount,
			TotalSize: leftDs.Structure.Length,
		}
	} else {
		res.Summary.Left = EmptyObject{}
	}
	if rightDs.Structure != nil {
		rightColItems, _, sErr = tabular.ColumnsFromJSONSchema(rightDs.Structure.Schema)
		if sErr != nil {
			return &StatsChangeComponent{}, sErr
		}
		rightColCount = len(rightColItems)

		res.Summary.Right = StatsChangeSummaryFields{
			Entries:   rightDs.Structure.Entries,
			Columns:   rightColCount,
			TotalSize: rightDs.Structure.Length,
		}
	} else {
		res.Summary.Right = EmptyObject{}
	}

	if leftDs.Structure != nil && rightDs.Structure != nil {
		res.Summary.Delta = StatsChangeSummaryFields{
			Entries:   res.Summary.Right.(StatsChangeSummaryFields).Entries - res.Summary.Left.(StatsChangeSummaryFields).Entries,
			Columns:   rightColCount - leftColCount,
			TotalSize: res.Summary.Right.(StatsChangeSummaryFields).TotalSize - res.Summary.Left.(StatsChangeSummaryFields).TotalSize,
		}
	} else if leftDs.Structure != nil {
		res.Summary.Delta = StatsChangeSummaryFields{
			Entries:   -res.Summary.Left.(StatsChangeSummaryFields).Entries,
			Columns:   rightColCount - leftColCount,
			TotalSize: -res.Summary.Left.(StatsChangeSummaryFields).TotalSize,
		}
	} else if rightDs.Structure != nil {
		res.Summary.Delta = StatsChangeSummaryFields{
			Entries:   res.Summary.Right.(StatsChangeSummaryFields).Entries,
			Columns:   rightColCount - leftColCount,
			TotalSize: res.Summary.Right.(StatsChangeSummaryFields).TotalSize,
		}
	} else {
		res.Summary.Delta = EmptyObject{}
	}

	maxColCount := leftColCount
	if rightColCount > maxColCount {
		maxColCount = rightColCount
	}

	res.Columns = make([]*ChangeReportDeltaComponent, maxColCount)

	type cIndex struct {
		LPos int
		RPos int
	}

	colIndex := map[string]*cIndex{}
	for i := 0; i < maxColCount; i++ {
		if i < leftColCount {
			if c, ok := colIndex[leftColItems[i].Title]; ok && c != nil {
				colIndex[leftColItems[i].Title].LPos = i
			} else {
				colIndex[leftColItems[i].Title] = &cIndex{
					LPos: i,
					RPos: -1,
				}
			}
		}
		if i < rightColCount {
			if c, ok := colIndex[rightColItems[i].Title]; ok && c != nil {
				colIndex[rightColItems[i].Title].RPos = i
			} else {
				colIndex[rightColItems[i].Title] = &cIndex{
					LPos: -1,
					RPos: i,
				}
			}
		}
	}

	// TODO(arqu): try to load on demand if nil
	if leftDs.Stats == nil || rightDs.Stats == nil {
		var statsErr error
		if leftDs.Stats == nil {
			leftDs.Stats, statsErr = svc.stats.Stats(ctx, leftDs)
			if statsErr != nil {
				return nil, fmt.Errorf("missing stats components: %w", statsErr)
			}
		}
		if rightDs.Stats == nil {
			rightDs.Stats, statsErr = svc.stats.Stats(ctx, rightDs)
			if statsErr != nil {
				return nil, fmt.Errorf("missing stats components: %w", statsErr)
			}
		} else {
			log.Debugf("RIGHT FILL: %+v", rightDs.Stats)
		}
	}

	lsr, err := json.Marshal(leftDs.Stats.Stats)
	if err != nil {
		log.Debugf("failed to load stats: %s", err.Error())
		return nil, err
	}
	leftStats := []EmptyObject{}
	err = json.Unmarshal(lsr, &leftStats)
	if err != nil {
		log.Debugf("failed to parse stats: %s", err.Error())
		return nil, err
	}

	rsr, err := json.Marshal(rightDs.Stats.Stats)
	if err != nil {
		log.Debugf("failed to load stats: %s", err.Error())
		return nil, err
	}
	rightStats := []EmptyObject{}
	err = json.Unmarshal(rsr, &rightStats)
	if err != nil {
		log.Debugf("failed to parse stats: %s", err.Error())
		return nil, err
	}

	i := 0
	for k, v := range colIndex {
		res.Columns[i] = &ChangeReportDeltaComponent{
			Title: k,
		}
		var lCol, rCol *tabular.Column
		if v.LPos >= 0 {
			res.Columns[i].Left = leftStats[v.LPos]
			lCol = &leftColItems[v.LPos]
		} else {
			res.Columns[i].Left = EmptyObject{}
		}
		if v.RPos >= 0 {
			res.Columns[i].Right = rightStats[v.RPos]
			rCol = &rightColItems[v.RPos]
		} else {
			res.Columns[i].Right = EmptyObject{}
		}
		deltaCol, err := svc.columnStatsDelta(res.Columns[i].Left, res.Columns[i].Right, lCol, rCol, v.LPos >= 0, v.RPos >= 0)
		if err != nil {
			log.Debugf("error calculating stats delta: %s", err.Error())
			return nil, fmt.Errorf("failed to calculate stats column delta for %q", res.Columns[i].Title)
		}
		res.Columns[i].Delta = deltaCol
		i++
	}

	return res, nil
}

func (svc *Service) columnStatsDelta(left, right interface{}, lCol, rCol *tabular.Column, hasLeft, hasRight bool) (map[string]interface{}, error) {
	deltaCol := map[string]interface{}{}
	if hasLeft && hasRight {
		rightStatsMap := map[string]interface{}{}
		srsm, err := json.Marshal(right)
		if err != nil {
			log.Debugf("error marshaling stats")
			return nil, err
		}
		err = json.Unmarshal(srsm, &rightStatsMap)
		if err != nil {
			log.Debugf("error unmarshaling stats")
			return nil, err
		}
		leftStatsMap := map[string]interface{}{}
		slsm, err := json.Marshal(left)
		if err != nil {
			log.Debugf("error marshaling stats")
			return nil, err
		}
		err = json.Unmarshal(slsm, &leftStatsMap)
		if err != nil {
			log.Debugf("error unmarshaling stats")
			return nil, err
		}
		if (rCol.Type.HasType("number") || rCol.Type.HasType("integer")) && (lCol.Type.HasType("number") || lCol.Type.HasType("integer")) {
			deltaCol["count"] = rightStatsMap["count"].(float64) - leftStatsMap["count"].(float64)
			deltaCol["max"] = rightStatsMap["max"].(float64) - leftStatsMap["max"].(float64)
			deltaCol["min"] = rightStatsMap["min"].(float64) - leftStatsMap["min"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if rightStatsMap["median"] == nil || leftStatsMap["median"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["median"] = rightStatsMap["median"].(float64) - leftStatsMap["median"].(float64)
			if rightStatsMap["mean"] == nil || leftStatsMap["mean"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["mean"] = rightStatsMap["mean"].(float64) - leftStatsMap["mean"].(float64)
		} else if rCol.Type.HasType("string") && lCol.Type.HasType("string") {
			deltaCol["count"] = rightStatsMap["count"].(float64) - leftStatsMap["count"].(float64)
			deltaCol["maxLength"] = rightStatsMap["maxLength"].(float64) - leftStatsMap["maxLength"].(float64)
			deltaCol["minLength"] = rightStatsMap["minLength"].(float64) - leftStatsMap["minLength"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if rightStatsMap["unique"] == nil || leftStatsMap["unique"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["unique"] = rightStatsMap["unique"].(float64) - leftStatsMap["unique"].(float64)
		} else if rCol.Type.HasType("bool") && lCol.Type.HasType("bool") {
			deltaCol["count"] = rightStatsMap["count"].(float64) - leftStatsMap["count"].(float64)
			deltaCol["trueCount"] = rightStatsMap["trueCount"].(float64) - leftStatsMap["trueCount"].(float64)
			deltaCol["falseCount"] = rightStatsMap["falseCount"].(float64) - leftStatsMap["falseCount"].(float64)
		} else {
			// TODO(arqu): improve handling of columns with different types
			return nil, fmt.Errorf("incompatible column types")
		}
	} else if hasLeft {
		leftStatsMap := map[string]interface{}{}
		slsm, err := json.Marshal(left)
		if err != nil {
			log.Debugf("error marshaling stats")
			return nil, err
		}
		err = json.Unmarshal(slsm, &leftStatsMap)
		if err != nil {
			log.Debugf("error unmarshaling stats")
			return nil, err
		}
		if lCol.Type.HasType("number") || lCol.Type.HasType("integer") {
			deltaCol["count"] = -leftStatsMap["count"].(float64)
			deltaCol["max"] = -leftStatsMap["max"].(float64)
			deltaCol["min"] = -leftStatsMap["min"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if leftStatsMap["median"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["median"] = -leftStatsMap["median"].(float64)
			if leftStatsMap["mean"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["mean"] = -leftStatsMap["mean"].(float64)
		} else if lCol.Type.HasType("string") {
			deltaCol["count"] = -leftStatsMap["count"].(float64)
			deltaCol["maxLength"] = -leftStatsMap["maxLength"].(float64)
			deltaCol["minLength"] = -leftStatsMap["minLength"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if leftStatsMap["unique"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["unique"] = -leftStatsMap["unique"].(float64)
		} else if lCol.Type.HasType("bool") {
			deltaCol["count"] = -leftStatsMap["count"].(float64)
			deltaCol["trueCount"] = -leftStatsMap["trueCount"].(float64)
			deltaCol["falseCount"] = -leftStatsMap["falseCount"].(float64)
		}
	} else if hasRight {
		rightStatsMap := map[string]interface{}{}
		srsm, err := json.Marshal(right)
		if err != nil {
			log.Debugf("error marshaling stats")
			return nil, err
		}
		err = json.Unmarshal(srsm, &rightStatsMap)
		if err != nil {
			log.Debugf("error unmarshaling stats")
			return nil, err
		}
		if rCol.Type.HasType("number") || rCol.Type.HasType("integer") {
			deltaCol["count"] = rightStatsMap["count"].(float64)
			deltaCol["max"] = rightStatsMap["max"].(float64)
			deltaCol["min"] = rightStatsMap["min"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if rightStatsMap["median"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["median"] = rightStatsMap["median"].(float64)
			if rightStatsMap["mean"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["mean"] = rightStatsMap["mean"].(float64)
		} else if rCol.Type.HasType("string") {
			deltaCol["count"] = rightStatsMap["count"].(float64)
			deltaCol["maxLength"] = rightStatsMap["maxLength"].(float64)
			deltaCol["minLength"] = rightStatsMap["minLength"].(float64)
			// TODO(arqu): investigate further - the bellow stats can be null
			// and silently fail the request if the stats accumulator was not closed propperly
			if rightStatsMap["unique"] == nil {
				return nil, fmt.Errorf("nil stats values")
			}
			deltaCol["unique"] = rightStatsMap["unique"].(float64)
		} else if rCol.Type.HasType("bool") {
			deltaCol["count"] = rightStatsMap["count"].(float64)
			deltaCol["trueCount"] = rightStatsMap["trueCount"].(float64)
			deltaCol["falseCount"] = rightStatsMap["falseCount"].(float64)
		}
	} else {
		return nil, fmt.Errorf("no left or right column present")
	}

	return deltaCol, nil
}

// Report computes the change report of two sources
// This takes some assumptions - we work only with tabular data, with header rows and functional structure.json
func (svc *Service) Report(ctx context.Context, leftRef, rightRef dsref.Ref, loadSource string) (*ChangeReportResponse, error) {
	leftDs, err := svc.loader.LoadDataset(ctx, leftRef, loadSource)
	if err != nil {
		return nil, err
	}
	if rightRef.Path == "" {
		rightRef.Path = leftDs.PreviousPath
	}
	rightDs, err := svc.loader.LoadDataset(ctx, rightRef, loadSource)
	if err != nil {
		return nil, err
	}

	res := &ChangeReportResponse{}

	leftVi := dsref.ConvertDatasetToVersionInfo(leftDs)
	rightVi := dsref.ConvertDatasetToVersionInfo(rightDs)

	res.VersionInfo = &ChangeReportComponent{}
	res.VersionInfo.Left = leftVi
	res.VersionInfo.Right = rightVi
	res.VersionInfo.Meta = EmptyObject{}

	if leftVi.Path == rightVi.Path {
		res.VersionInfo.Meta["status"] = fsi.STUnmodified
	} else {
		res.VersionInfo.Meta["status"] = fsi.STChange
	}

	if leftDs.Commit != nil || rightDs.Commit != nil {
		res.Commit = &ChangeReportComponent{}
		if leftDs.Commit != nil {
			res.Commit.Left = leftDs.Commit
		} else {
			res.Commit.Left = EmptyObject{}
		}
		if rightDs.Commit != nil {
			res.Commit.Right = rightDs.Commit
		} else {
			res.Commit.Right = EmptyObject{}
		}
		res.Commit.Meta = EmptyObject{}

		if leftDs.Commit != nil && rightDs.Commit == nil {
			res.Commit.Meta["status"] = fsi.STRemoved
		} else if leftDs.Commit == nil && rightDs.Commit != nil {
			res.Commit.Meta["status"] = fsi.STAdd
		} else if leftDs.Commit != nil && rightDs.Commit != nil {
			if leftDs.Commit.Path == rightDs.Commit.Path {
				res.Commit.Meta["status"] = fsi.STUnmodified
			} else {
				res.Commit.Meta["status"] = fsi.STChange
			}
		} else {
			res.Commit.Meta["status"] = fsi.STParseError
		}
	}

	if leftDs.Meta != nil || rightDs.Meta != nil {
		res.Meta = &ChangeReportComponent{}
		hasLeftMeta := leftDs.Meta != nil && !leftDs.Meta.IsEmpty()
		hasRightMeta := rightDs.Meta != nil && !rightDs.Meta.IsEmpty()

		if hasLeftMeta {
			res.Meta.Left = leftDs.Meta
		} else {
			res.Meta.Left = EmptyObject{}
		}
		if hasRightMeta {
			res.Meta.Right = rightDs.Meta
		} else {
			res.Meta.Right = EmptyObject{}
		}
		res.Meta.Meta = EmptyObject{}

		if hasLeftMeta && !hasRightMeta {
			res.Meta.Meta["status"] = fsi.STRemoved
		} else if !hasLeftMeta && hasRightMeta {
			res.Meta.Meta["status"] = fsi.STAdd
		} else if hasLeftMeta && hasRightMeta {
			if leftDs.Meta.Path == rightDs.Meta.Path {
				res.Meta.Meta["status"] = fsi.STUnmodified
			} else {
				res.Meta.Meta["status"] = fsi.STChange
			}
		} else {
			res.Meta.Meta["status"] = fsi.STParseError
		}
	}

	if leftDs.Readme != nil || rightDs.Readme != nil {
		res.Readme = &ChangeReportComponent{}
		if leftDs.Readme != nil {
			res.Readme.Left = string(leftDs.Readme.ScriptBytes)
		} else {
			res.Readme.Left = ""
		}
		if rightDs.Readme != nil {
			res.Readme.Right = string(rightDs.Readme.ScriptBytes)
		} else {
			res.Readme.Right = ""
		}
		res.Readme.Meta = EmptyObject{}

		if res.Readme.Left != "" && res.Readme.Right == "" {
			res.Readme.Meta["status"] = fsi.STRemoved
		} else if res.Readme.Left == "" && res.Readme.Right != "" {
			res.Readme.Meta["status"] = fsi.STAdd
		} else if res.Readme.Left != "" && res.Readme.Right != "" {
			if res.Readme.Left == res.Readme.Right {
				res.Readme.Meta["status"] = fsi.STUnmodified
			} else {
				res.Readme.Meta["status"] = fsi.STChange
			}
		} else {
			res.Readme.Meta["status"] = fsi.STParseError
		}
	}

	if leftDs.Structure != nil || rightDs.Structure != nil {
		res.Structure = &ChangeReportComponent{}
		if leftDs.Structure != nil {
			res.Structure.Left = leftDs.Structure
		} else {
			res.Structure.Left = EmptyObject{}
		}
		if rightDs.Structure != nil {
			res.Structure.Right = rightDs.Structure
		} else {
			res.Structure.Right = EmptyObject{}
		}
		res.Structure.Meta = EmptyObject{}

		if leftDs.Structure != nil && rightDs.Structure == nil {
			res.Structure.Meta["status"] = fsi.STRemoved
		} else if leftDs.Structure == nil && rightDs.Structure != nil {
			res.Structure.Meta["status"] = fsi.STAdd
		} else if leftDs.Structure != nil && rightDs.Structure != nil {
			if leftDs.Structure.Path == rightDs.Structure.Path {
				res.Structure.Meta["status"] = fsi.STUnmodified
			} else {
				res.Structure.Meta["status"] = fsi.STChange
			}
		} else {
			res.Structure.Meta["status"] = fsi.STParseError
		}
	}

	if leftDs.Transform != nil || rightDs.Transform != nil {
		res.Transform = &ChangeReportComponent{}
		if leftDs.Transform != nil {
			res.Transform.Left = string(leftDs.Transform.ScriptBytes)
		} else {
			res.Transform.Left = ""
		}
		if rightDs.Transform != nil {
			res.Transform.Right = string(rightDs.Transform.ScriptBytes)
		} else {
			res.Transform.Right = ""
		}
		res.Transform.Meta = EmptyObject{}

		if res.Transform.Left != "" && res.Transform.Right == "" {
			res.Transform.Meta["status"] = fsi.STRemoved
		} else if res.Transform.Left == "" && res.Transform.Right != "" {
			res.Transform.Meta["status"] = fsi.STAdd
		} else if res.Transform.Left != "" && res.Transform.Right != "" {
			if res.Transform.Left == res.Transform.Right {
				res.Transform.Meta["status"] = fsi.STUnmodified
			} else {
				res.Transform.Meta["status"] = fsi.STChange
			}
		} else {
			res.Transform.Meta["status"] = fsi.STParseError
		}
	}

	res.Stats, err = svc.statsDiff(ctx, leftDs, rightDs)
	if err != nil {
		return nil, err
	}
	return res, nil
}
