package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/msam1r/distribkv/config"
	"github.com/msam1r/distribkv/db"
	"github.com/msam1r/distribkv/web"
)

var (
	dbPath     = flag.String("db-path", "", "The database location")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "The HTTP Host & Port")
	configFile = flag.String("config-file", "sharding.toml", "Static Sharding Config File")
	shard      = flag.String("shard", "", "The name of the shard")
)

func parseFlags() {
	flag.Parse()

	if *dbPath == "" {
		log.Fatalf("Must provide db-location.")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard name")
	}
}

func main() {
	parseFlags()

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	db, close, err := db.NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v", *dbPath, err)
	}
	defer close()

	s := web.NewServer(db, shards)
	http.HandleFunc("/get", s.GetHandler)
	http.HandleFunc("/set", s.SetHandler)
	http.HandleFunc("/purge", s.DeleteExtraKeysHandler)

	log.Printf("Starting server at http://%s\n", *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
