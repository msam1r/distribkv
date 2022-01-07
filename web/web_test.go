package web_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/msam1r/distribkv/config"
	"github.com/msam1r/distribkv/db"
	"github.com/msam1r/distribkv/web"
)

func createShardDB(t *testing.T, idx int) *db.Database {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "test.db")
	if err != nil {
		t.Fatalf("Could not create temp database. err: %v", err)
	}
	f.Close()

	t.Cleanup(func() {
		os.Remove(f.Name())
	})

	db, closeFunc, err := db.NewDatabase(f.Name())
	if err != nil {
		t.Fatalf("Could not create database. err: %v", err)
	}

	t.Cleanup(func() { closeFunc() })

	return db
}

func createShardServer(t *testing.T, idx int, addrs map[int]string) (*web.Server, *db.Database) {
	t.Helper()

	db := createShardDB(t, idx)
	shards := &config.Shards{
		Count:      len(addrs),
		CurrentIdx: idx,
		Addrs:      addrs,
	}

	return web.NewServer(db, shards), db
}

func createTestHttpServer(t *testing.T, tsGetHandler, tsSetHandler *http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			funcs := *tsGetHandler
			funcs(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			funcs := *tsSetHandler
			funcs(w, r)
		}
	}))
}

func TestWebServer(t *testing.T) {
	var ts1GetHandler, ts1SetHandler http.HandlerFunc
	var ts2GetHandler, ts2SetHandler http.HandlerFunc

	ts1 := createTestHttpServer(t, &ts1GetHandler, &ts1SetHandler)
	defer ts1.Close()

	ts2 := createTestHttpServer(t, &ts2GetHandler, &ts2SetHandler)
	defer ts2.Close()

	addrs := map[int]string{
		0: strings.TrimPrefix(ts1.URL, "http://"),
		1: strings.TrimPrefix(ts2.URL, "http://"),
	}

	web1, db1 := createShardServer(t, 0, addrs)
	web2, db2 := createShardServer(t, 1, addrs)

	keys := map[string]int{
		"Giza":  0,
		"Minia": 1,
	}

	ts1GetHandler = web1.GetHandler
	ts1SetHandler = web1.SetHandler
	ts2GetHandler = web2.GetHandler
	ts2SetHandler = web2.SetHandler

	// This should redirect to another shard on one of the keys.
	// See the tests log to know which one is redirected.
	for key := range keys {
		resp, err := http.Get(fmt.Sprintf(ts1.URL+"/set?key=%s&value=value-%s", key, key))
		if err != nil {
			t.Fatalf("Could not set the key %q: %v", key, err)
		}

		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Set: Could read contents of the key %q: %v", key, err)
		}

		log.Printf("Set: Contents of key %q: %s\n", key, contents)
	}

	// This should redirect to another shard on one of the keys.
	// See the tests log to know which one is redirected.
	for key := range keys {
		resp, err := http.Get(fmt.Sprintf(ts1.URL+"/get?key=%s", key))
		if err != nil {
			t.Fatalf("Get key %q error: %v", key, err)
		}
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Could read contents of the key %q: %v", key, err)
		}

		want := []byte("value-" + key)
		if !bytes.Contains(contents, want) {
			t.Errorf("Unexpected contents of the key %q: got %q, want the result to contain %q", key, contents, want)
		}

		log.Printf("Get: Contents of key %q: %s", key, contents)
	}

	// test the values from the database itself.
	want1 := "value-Giza"
	got1, err := db1.Get("Giza")
	if err != nil {
		t.Fatalf("Giza key error: %v", err)
	}

	if !bytes.Equal([]byte(want1), got1) {
		t.Errorf("Unexpected value. got: %q, want: %q", got1, want1)
	}

	want2 := "value-Minia"
	got2, err := db2.Get("Minia")
	if err != nil {
		t.Fatalf("Minia key error: %v", err)
	}

	if !bytes.Equal([]byte(want2), got2) {
		t.Errorf("Unexpected value. got: %q, want: %q", got2, want2)
	}
}
