package api

import (
	// "math/rand"
	"context"
	"testing"
)

func TestRemoteClientHandlers(t *testing.T) {
	t.Skip("TODO(dlong): Skip for now, returning a 500, need to investigate")

	node, teardown := newTestNodeWithNumDatasets(t, 2)
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inst := newTestInstanceWithProfileFromNode(ctx, node)
	l := NewLogHandlers(inst)
	h := NewRemoteClientHandlers(inst, false)

	publishCases := []handlerTestCase{
		{"GET", "/publish/", nil},
		{"POST", "/publish/me/cities", nil},
		{"DELETE", "/publish/me/cities", nil},
	}
	runHandlerTestCases(t, "publish", h.PushHandler, publishCases, true)

	// tests getting a list of logs from a remote
	fetchCases := []handlerTestCase{
		{"GET", "/history/", nil},
		{"GET", "/history/me/cities", nil},
	}
	runHandlerTestCases(t, "fetch", l.LogHandler, fetchCases, true)
}
