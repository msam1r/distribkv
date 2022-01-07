package config

import (
	"fmt"
	"hash/fnv"

	"github.com/BurntSushi/toml"
)

type Shard struct {
	Name    string
	Idx     int
	Address string
}

type Config struct {
	Shards []Shard
}

type Shards struct {
	Count      int
	CurrentIdx int
	Addrs      map[int]string
}

func ParseFile(filename string) (Config, error) {
	var c Config
	_, err := toml.DecodeFile(filename, &c)
	if err != nil {
		return Config{}, err
	}

	return c, nil
}

func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)

	for _, s := range shards {
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		addrs[s.Idx] = s.Address
		if s.Name == curShardName {
			shardIdx = s.Idx
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", curShardName)
	}

	return &Shards{
		Count:      shardCount,
		CurrentIdx: shardIdx,
		Addrs:      addrs,
	}, nil
}

// GetShard returns the shard index for the given key.
func (s *Shards) GetShard(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}
