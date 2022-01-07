package config_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/msam1r/distribkv/config"
)

func createConfig(t *testing.T, content string) config.Config {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "sharding.toml")
	if err != nil {
		t.Fatalf("Could not create a temp file: %v", err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	_, err = f.WriteString(content)
	if err != nil {
		t.Fatalf("Could not write the config content: %v", err)
	}

	c, err := config.ParseFile(f.Name())
	if err != nil {
		t.Fatalf("Could not parse config %#v", c)
	}

	return c
}

func TestConfigParse(t *testing.T) {
	got := createConfig(t, `[[shards]]
		name = "Minia"
		idx = 0
		address = "127.0.0.1:8080"`)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "Minia",
				Idx:     0,
				Address: "127.0.0.1:8080",
			},
		},
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("The config does not match. got: %#v, want: %#v", got, want)
	}
}

func TestParseShard(t *testing.T) {
	c := createConfig(t, `[[shards]]
		name = "Minia"
		idx = 0
		address = "127.0.0.1:8080"
		[[shards]]
		name = "Cairo"
		idx = 1
		address = "127.0.0.1:8081"`)

	got, err := config.ParseShards(c.Shards, "Cairo")
	if err != nil {
		t.Fatalf("Got error while parsing the shards")
	}

	want := &config.Shards{
		Count:      2,
		CurrentIdx: 1,
		Addrs: map[int]string{
			0: "127.0.0.1:8080",
			1: "127.0.0.1:8081",
		},
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("The shards does not match. got: %v, want: %v", got, want)
	}
}
