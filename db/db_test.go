package db_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/msam1r/distribkv/db"
)

func createTestDatabase(t *testing.T) *db.Database {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "test.db")
	if err != nil {
		t.Fatalf("Could not create test database. err: %v", err)
	}
	f.Close()
	name := f.Name()

	db, closeFunc, err := db.NewDatabase(name)
	if err != nil {
		t.Fatalf("Could not create new database. err: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(name)
		closeFunc()
	})

	return db
}

func setKey(t *testing.T, db *db.Database, key string, value []byte) {
	t.Helper()

	if err := db.Set(key, value); err != nil {
		t.Fatalf("Could not write key. err: %v", err)
	}
}

func getKey(t *testing.T, db *db.Database, key string) []byte {
	t.Helper()

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Could not get key. err: %v", err)
	}

	return got
}

func TestSetGetDelete(t *testing.T) {
	db := createTestDatabase(t)

	want := []byte("Mohamed Samir")
	setKey(t, db, "name", want)

	got := getKey(t, db, "name")

	if !bytes.Equal(got, want) {
		t.Errorf("Keys does not match. got: %v, want: %v", got, want)
	}

	err := db.Delete("name")
	if err != nil {
		t.Fatalf("Could not delete key. err: %v", err)
	}

	got = getKey(t, db, "name")
	if string(got) != "" {
		t.Fatalf(`Key "name" not deleted.`)
	}
}

func TestDeleteExtraKeys(t *testing.T) {

	db := createTestDatabase(t)

	name := []byte("mohamed samir")
	email := []byte("gm.mohamedsamir@gmail.com")

	setKey(t, db, "name", name)
	setKey(t, db, "email", email)

	err := db.DeleteExtraKeys(func(key string) bool {
		return key == "email"
	})
	if err != nil {
		t.Fatalf("Could not delete extra keys. err: %v", err)
	}

	value := getKey(t, db, "name")
	if !bytes.Equal(value, name) {
		t.Fatalf("unexpected value. got: %q, want: %q", value, name)
	}

	value = getKey(t, db, "email")
	if string(value) != "" {
		t.Fatalf("unexpected value. got: %q, want: %q", value, "")
	}
}
