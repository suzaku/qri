// Package api implements a JSON-API for interacting with a qri node
package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	golog "github.com/ipfs/go-log"
	apiutil "github.com/qri-io/qri/api/util"
	"github.com/qri-io/qri/lib"
	"github.com/qri-io/qri/version"
)

var (
	log = golog.Logger("qriapi")
	// APIVersion is the version string that is written in API responses
	APIVersion = version.String
)

const (
	// DefaultTemplateHash is the hash of the default render template
	DefaultTemplateHash = "/ipfs/QmeqeRTf2Cvkqdx4xUdWi1nJB2TgCyxmemsL3H4f1eTBaw"
	// TemplateUpdateAddress is the URI for the template update
	TemplateUpdateAddress = "/ipns/defaulttmpl.qri.io"
)

func init() {
	golog.SetLogLevel("qriapi", "info")
}

// Server wraps a qri p2p node, providing traditional access via http
// Create one with New, start it up with Serve
type Server struct {
	*lib.Instance
}

// New creates a new qri server from a p2p node & configuration
func New(inst *lib.Instance) (s Server) {
	return Server{Instance: inst}
}

// Serve starts the server. It will block while the server is running
func (s Server) Serve(ctx context.Context) (err error) {
	node := s.Node()
	cfg := s.Config()

	if err := s.Instance.Connect(ctx); err != nil {
		return err
	}

	server := &http.Server{}
	mux := NewServerRoutes(s)
	server.Handler = mux

	go s.ServeRPC(ctx)
	go s.ServeWebsocket(ctx)

	info := "\n📡  Success! You are now connected to the d.web. Here's your connection details:\n"
	info += cfg.SummaryString()
	info += "IPFS Addresses:"
	for _, a := range node.EncapsulatedAddresses() {
		info = fmt.Sprintf("%s\n  %s", info, a.String())
	}
	info += fmt.Sprintf("\nYou are running Qri v%s", APIVersion)
	info += "\n\n"

	node.LocalStreams.Print(info)

	if cfg.API.DisconnectAfter != 0 {
		log.Infof("disconnecting after %d seconds", cfg.API.DisconnectAfter)
		go func(s *http.Server, t int) {
			<-time.After(time.Second * time.Duration(t))
			log.Infof("disconnecting")
			s.Close()
		}(server, cfg.API.DisconnectAfter)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		server.Close()
	}()

	// http.ListenAndServe will not return unless there's an error
	return StartServer(cfg.API, server)
}

// HandleIPFSPath responds to IPFS Hash requests with raw data
func (s *Server) HandleIPFSPath(w http.ResponseWriter, r *http.Request) {
	if s.Config().API.ReadOnly {
		readOnlyResponse(w, "/ipfs/")
		return
	}

	file, err := s.Node().Repo.Filesystem().Get(r.Context(), r.URL.Path)
	if err != nil {
		apiutil.WriteErrResponse(w, http.StatusInternalServerError, err)
		return
	}

	io.Copy(w, file)
}

// helper function
func readOnlyResponse(w http.ResponseWriter, endpoint string) {
	apiutil.WriteErrResponse(w, http.StatusForbidden, fmt.Errorf("qri server is in read-only mode, access to '%s' endpoint is forbidden", endpoint))
}

// HomeHandler responds with a health check on the empty path, 404 for
// everything else
func HomeHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "" || r.URL.Path == "/" {
		HealthCheckHandler(w, r)
		return
	}

	apiutil.NotFoundHandler(w, r)
}

// HealthCheckHandler is a basic ok response for load balancers & co
// returns the version of qri this node is running, pulled from the lib package
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{ "meta": { "code": 200, "status": "ok", "version":"` + APIVersion + `" }, "data": [] }`))
}

// NewServerRoutes returns a Muxer that has all API routes
func NewServerRoutes(s Server) *http.ServeMux {
	cfg := s.Config()

	m := http.NewServeMux()

	m.Handle("/", s.noLogMiddleware(HomeHandler))
	m.Handle("/health", s.noLogMiddleware(HealthCheckHandler))
	m.Handle("/ipfs/", s.middleware(s.HandleIPFSPath))

	proh := NewProfileHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/me", s.middleware(proh.ProfileHandler))
	m.Handle("/profile", s.middleware(proh.ProfileHandler))
	m.Handle("/profile/photo", s.middleware(proh.ProfilePhotoHandler))
	m.Handle("/profile/poster", s.middleware(proh.PosterHandler))

	ph := NewPeerHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/peers", s.middleware(ph.PeersHandler))
	m.Handle("/peers/", s.middleware(ph.PeerHandler))
	m.Handle("/connect/", s.middleware(ph.ConnectToPeerHandler))
	m.Handle("/connections", s.middleware(ph.ConnectionsHandler))

	if cfg.Remote != nil && cfg.Remote.Enabled {
		log.Info("running in `remote` mode")

		remh := NewRemoteHandlers(s.Instance)
		m.Handle("/remote/dsync", s.middleware(remh.DsyncHandler))
		m.Handle("/remote/logsync", s.middleware(remh.LogsyncHandler))
		m.Handle("/remote/refs", s.middleware(remh.RefsHandler))
	}

	dsh := NewDatasetHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/list", s.middleware(dsh.ListHandler))
	m.Handle("/list/", s.middleware(dsh.PeerListHandler))
	m.Handle("/save", s.middleware(dsh.SaveHandler))
	m.Handle("/save/", s.middleware(dsh.SaveHandler))
	m.Handle("/remove/", s.middleware(dsh.RemoveHandler))
	m.Handle("/get/", s.middleware(dsh.GetHandler))
	m.Handle("/rename", s.middleware(dsh.RenameHandler))
	m.Handle("/diff", s.middleware(dsh.DiffHandler))
	m.Handle("/changes", s.middleware(dsh.ChangesHandler))
	// Deprecated, use /get/username/name?component=body or /get/username/name/body.csv
	m.Handle("/body/", s.middleware(dsh.BodyHandler))
	m.Handle("/stats/", s.middleware(dsh.StatsHandler))
	m.Handle("/unpack/", s.middleware(dsh.UnpackHandler))

	remClientH := NewRemoteClientHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/push/", s.middleware(remClientH.PushHandler))
	m.Handle("/pull/", s.middleware(dsh.PullHandler))
	m.Handle("/feeds", s.middleware(remClientH.FeedsHandler))
	m.Handle("/preview/", s.middleware(remClientH.DatasetPreviewHandler))

	fsih := NewFSIHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/status/", s.middleware(fsih.StatusHandler("/status")))
	m.Handle("/whatchanged/", s.middleware(fsih.WhatChangedHandler("/whatchanged")))
	m.Handle("/init/", s.middleware(fsih.InitHandler("/init")))
	m.Handle("/checkout/", s.middleware(fsih.CheckoutHandler("/checkout")))
	m.Handle("/restore/", s.middleware(fsih.RestoreHandler("/restore")))
	m.Handle("/fsi/write/", s.middleware(fsih.WriteHandler("/fsi/write")))

	renderh := NewRenderHandlers(s.Instance)
	m.Handle("/render", s.middleware(renderh.RenderHandler))
	m.Handle("/render/", s.middleware(renderh.RenderHandler))

	lh := NewLogHandlers(s.Instance)
	m.Handle("/history/", s.middleware(lh.LogHandler))

	rch := NewRegistryClientHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/registry/profile/new", s.middleware(rch.CreateProfileHandler))
	m.Handle("/registry/profile/prove", s.middleware(rch.ProveProfileKeyHandler))

	sh := NewSearchHandlers(s.Instance)
	m.Handle("/search", s.middleware(sh.SearchHandler))

	sqlh := NewSQLHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle("/sql", s.middleware(sqlh.QueryHandler("/sql")))

	if !cfg.API.DisableWebui {
		m.Handle("/webui", s.middleware(WebuiHandler))
	}

	return m
}
