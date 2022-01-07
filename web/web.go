package web

import (
	"fmt"
	"io"
	"net/http"

	"github.com/msam1r/distribkv/config"
	"github.com/msam1r/distribkv/db"
)

type Server struct {
	db     *db.Database
	shards *config.Shards
}

func NewServer(db *db.Database, shards *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: shards,
	}
}

// redirect handle the redirection to another shard with the given shard index.
func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI

	fmt.Fprintf(w, "Redirecting from shard %d to shard %d\n", s.shards.CurrentIdx, shard)

	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.GetShard(key)
	if s.shards.CurrentIdx != shard {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.Get(key)
	fmt.Fprintf(w, "Value = %q, Error = %v", value, err)
}

func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.GetShard(key)
	if s.shards.CurrentIdx != shard {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.Set(key, []byte(value))
	fmt.Fprintf(w, "Error = %v", err)
}

func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	err := s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.GetShard(key) != s.shards.CurrentIdx
	})

	fmt.Fprintf(w, "Error = %v", err)
}
