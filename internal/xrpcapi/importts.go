package xrpcapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/importer"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/jcalabro/gt"
)

// ImportManager is the subset of *importer.Manager the XRPC surface needs.
// Defined as an interface so the handlers can be tested with a fake and so
// xrpcapi does not force an orchestrator dependency on callers that leave
// import disabled.
type ImportManager interface {
	// Submit validates + confines path and launches an async job, returning
	// its id. runCtx roots the background run.
	Submit(runCtx context.Context, path string) (string, error)
	// Status returns the job record for id.
	Status(id string) (importer.Record, error)
	// Current returns the active/most-recent job record, if any.
	Current() (importer.Record, bool)
}

// newImportTimestampsHandler builds the importTimestamps procedure handler.
// runCtx roots every submitted job's background run so jobs are cancelled on
// server shutdown (and then auto-resume on the next boot).
func newImportTimestampsHandler(mgr ImportManager, runCtx context.Context) xrpcserver.Handler {
	return xrpcserver.Procedure(func(_ context.Context, _ xrpcserver.Params, input *jetstream.JetstreamImportTimestamps_Input) (*jetstream.JetstreamImportTimestamps_Output, error) {
		if input == nil {
			return nil, xrpcserver.InvalidRequest("missing request body")
		}
		id, err := mgr.Submit(runCtx, input.Path)
		if err != nil {
			return nil, importSubmitError(err)
		}
		return &jetstream.JetstreamImportTimestamps_Output{Job: id}, nil
	})
}

// newGetImportStatusHandler builds the getImportStatus query handler.
func newGetImportStatusHandler(mgr ImportManager) xrpcserver.Handler {
	return xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*jetstream.JetstreamGetImportStatus_Output, error) {
		id := p.StringOr("job", "")
		var (
			rec importer.Record
			err error
		)
		if id == "" {
			var ok bool
			rec, ok = mgr.Current()
			if !ok {
				return nil, &xrpc.Error{StatusCode: http.StatusNotFound, Name: "JobNotFound", Message: "no import job has run"}
			}
		} else {
			rec, err = mgr.Status(id)
			if err != nil {
				if errors.Is(err, importer.ErrJobNotFound) {
					return nil, &xrpc.Error{StatusCode: http.StatusNotFound, Name: "JobNotFound", Message: "job not found"}
				}
				return nil, xrpcserver.InternalError("failed to read import status")
			}
		}
		return importStatusOutput(rec), nil
	})
}

// importSubmitError maps a manager Submit error to the lexicon-declared XRPC
// error names so clients matching on the published names work.
func importSubmitError(err error) error {
	switch {
	case errors.Is(err, importer.ErrJobInProgress):
		return &xrpc.Error{StatusCode: http.StatusConflict, Name: "ImportInProgress", Message: err.Error()}
	case errors.Is(err, importer.ErrPathRequired),
		errors.Is(err, importer.ErrPathEscape),
		errors.Is(err, importer.ErrPathNotFound),
		errors.Is(err, importer.ErrNotAFile):
		return &xrpc.Error{StatusCode: http.StatusBadRequest, Name: "InvalidPath", Message: err.Error()}
	default:
		return xrpcserver.InternalError("failed to submit import")
	}
}

func importStatusOutput(rec importer.Record) *jetstream.JetstreamGetImportStatus_Output {
	out := &jetstream.JetstreamGetImportStatus_Output{
		Job:                    rec.ID,
		State:                  string(rec.State),
		Bucketed:               gt.Some(rec.Bucketed),
		SegmentsToApply:        gt.Some(int64(rec.SegmentsToApply)),
		SegmentsApplied:        gt.Some(int64(rec.SegmentsApplied)),
		RowsTotal:              gt.Some(int64(rec.RowsTotal)),
		RowsValid:              gt.Some(int64(rec.RowsValid)),
		RowsRejected:           gt.Some(int64(rec.RowsRejected)),
		SegmentsExamined:       gt.Some(int64(rec.SegmentsExamined)),
		SegmentsPatched:        gt.Some(int64(rec.SegmentsPatched)),
		RowsMutated:            gt.Some(int64(rec.RowsMutated)),
		RowsMatchedAllVersions: gt.Some(int64(rec.RowsMatchedAllVersions)),
		RowsMatchedSpecific:    gt.Some(int64(rec.RowsMatchedSpecific)),
		SpecificCidsUnmatched:  gt.Some(int64(rec.SpecificCIDsUnmatched)),
		RowsCorruptOffset:      gt.Some(int64(rec.RowsCorruptOffset)),
	}
	if rec.Phase != "" {
		out.Phase = gt.Some(string(rec.Phase))
	}
	if rec.Error != "" {
		out.Error = gt.Some(rec.Error)
	}
	if !rec.SubmittedAt.IsZero() {
		out.SubmittedAt = gt.Some(rec.SubmittedAt.UTC().Format(rfc3339Millis))
	}
	if !rec.FinishedAt.IsZero() {
		out.FinishedAt = gt.Some(rec.FinishedAt.UTC().Format(rfc3339Millis))
	}
	return out
}

// rfc3339Millis matches the lexicon "datetime" format with millisecond
// precision (atproto-conventional).
const rfc3339Millis = "2006-01-02T15:04:05.000Z"
