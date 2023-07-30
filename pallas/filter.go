package pallas

import (
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
	"log"
)

const (
	COND_EQ = "cond_eq"
)

func eq(a, b any) bool {
	return a == b
}

type compFilter struct {
	kvMap M
	comp  func(a, b any) bool
}

func (f compFilter) filt(record M) bool {
	for k, v := range f.kvMap {
		value, ok := record[k]
		if !ok {
			return false
		}
		if k == "id" {
			return f.comp(value, v.(uint64))
		}
		return f.comp(value, v)
	}
	return true
}

type Filter struct {
	pallas      *Pallas
	bucket      string
	compFilters []compFilter
	selected    []string
	limit       int
}

func NewFilter(db *Pallas, bucket string) *Filter {
	return &Filter{
		pallas:      db,
		bucket:      bucket,
		compFilters: make([]compFilter, 0),
	}
}

func (f *Filter) Equal(kv M) *Filter {
	cp := compFilter{
		kvMap: kv,
		comp:  eq,
	}
	f.compFilters = append(f.compFilters, cp)
	return f
}

func (f *Filter) Limit(l int) *Filter {
	f.limit = l
	return f
}

func (f *Filter) Select(values ...string) *Filter {
	f.selected = append(f.selected, values...)
	return f
}

func (f *Filter) Insert(v M) (uint64, error) {
	tx, err := f.pallas.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	b, err := tx.CreateBucketIfNotExists([]byte(f.bucket))
	if err != nil {
		return 0, err
	}
	bid, err := b.NextSequence()
	if err != nil {
		return 0, err
	}
	data, err := f.pallas.Encoder.Encode(v)
	if err != nil {
		return 0, err
	}
	if err := b.Put(uint64Bytes(bid), data); err != nil {
		return 0, err
	}
	return bid, tx.Commit()
}

func (f *Filter) Update(kvs M) ([]M, error) {
	tx, err := f.pallas.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	b := tx.Bucket([]byte(f.bucket))
	if b == nil {
		return nil, fmt.Errorf("cannot find bucket %s", f.bucket)
	}
	records, err := f.findFiltered(b)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		for k, v := range kvs {
			if _, ok := record[k]; ok {
				record[k] = v
			}
		}
		data, err := f.pallas.Encoder.Encode(record)
		if err != nil {
			return nil, err
		}
		if err := b.Put(uint64Bytes(record["id"].(uint64)), data); err != nil {
			return nil, err
		}
	}
	return records, tx.Commit()
}

func (f *Filter) Find() ([]M, error) {
	tx, err := f.pallas.db.Begin(true)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket([]byte(f.bucket))
	if b == nil {
		return nil, fmt.Errorf("cannot find bucket %s", f.bucket)
	}
	records, err := f.findFiltered(b)
	log.Printf("[filter.Find] found records: %v", records)
	if err != nil {
		return nil, err
	}
	return records, tx.Commit()
}

func (f *Filter) Delete() error {
	tx, err := f.pallas.db.Begin(true)
	if err != nil {
		return err
	}
	b := tx.Bucket([]byte(f.bucket))
	if b == nil {
		return fmt.Errorf("cannot find bucket %s", f.bucket)
	}
	records, err := f.findFiltered(b)
	if err != nil {
		return err
	}
	for _, record := range records {
		id := uint64Bytes(record["id"].(uint64))
		if err := b.Delete(id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (f *Filter) findFiltered(bucket *bbolt.Bucket) ([]M, error) {
	var results []M
	bucket.ForEach(func(k, v []byte) error {
		record := M{
			"id": bytes2uint64(k),
		}
		if err := f.pallas.Decoder.Decode(v, &record); err != nil {
			return err
		}
		include := true
		for _, filter := range f.compFilters {
			if !filter.filt(record) {
				include = false
				break
			}
		}
		if !include {
			return nil
		}
		record = f.selectFrom(record)
		results = append(results, record)
		return nil
	})
	return results, nil
}

func (f *Filter) selectFrom(record M) M {
	if len(f.selected) == 0 {
		return record
	}
	data := M{}
	for _, key := range f.selected {
		if _, ok := record[key]; ok {
			data[key] = record[key]
		}
	}
	return data
}

func uint64Bytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func bytes2uint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
