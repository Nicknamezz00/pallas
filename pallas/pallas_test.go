package pallas

import (
	"log"
	"testing"
)

func TestDelete(t *testing.T) {
	db, err := NewPallas(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Drop("test")
	id, err := db.Bucket("users").Insert(M{"name": "foo"})
	if err != nil {
		t.Fatal(err)
	}
	delete := M{"id": id}
	if err := db.Bucket("users").Equal(delete).Delete(); err != nil {
		t.Fatal(err)
	}
	records, err := db.Bucket("users").Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("want 0 records, got %d records", len(records))
	}
}

func TestUpdate(t *testing.T) {
	db, err := NewPallas(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Drop("test")
	_, err = db.Bucket("users").Insert(M{"name": "foo"})
	if err != nil {
		t.Fatal(err)
	}
	values := M{"name": "bar"}
	res, err := db.Bucket("users").Update(values)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		log.Fatalf("want 1 result, got %d result", len(res))
	}
	records, err := db.Bucket("users").Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		log.Fatalf("want 1 record, got %d record", len(records))
	}
	if records[0]["name"] != values["name"] {
		t.Fatalf("want name to be %s, got %s", values["name"], records[0]["name"])
	}
}

func TestInsert(t *testing.T) {
	values := []M{
		{
			"name": "Foo",
			"age":  10,
		},
		{
			"name": "Bar",
			"age":  12,
		},
		{
			"name": "Baz",
			"age":  20,
		},
	}
	db, err := NewPallas(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Drop("test")
	for i, data := range values {
		id, err := db.Bucket("users").Insert(data)
		if err != nil {
			t.Fatal(err)
		}
		if id != uint64(i+1) {
			t.Fatalf("want ID to be %d, got %d", i+1, id)
		}
	}
	users, err := db.Bucket("users").Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != len(values) {
		t.Fatalf("want %d users, got %d users", len(values), len(users))
	}
}

func TestFind(t *testing.T) {
	db, err := NewPallas(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Drop("test")

	db.Bucket("users").Insert(M{"username": "ABE"})
	db.Bucket("users").Insert(M{"username": "Bob"})
	db.Bucket("users").Insert(M{"username": "Alice"})
	db.Bucket("users").Insert(M{"username": "John"})
	results, err := db.Bucket("users").Equal(M{"username": "Alice"}).Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d result", len(results))
	}
}
