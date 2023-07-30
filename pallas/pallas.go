package pallas

import (
	"encoding/json"
	"fmt"
	"go.etcd.io/bbolt"
	"os"
)

const (
	defaultDBName = "default"
	dbExtension   = "pallas"
)

type M map[string]any

type Pallas struct {
	db *bbolt.DB

	*Options
}

func NewPallas(options ...Option) (*Pallas, error) {
	opts := &Options{
		DBName:  defaultDBName,
		Encoder: JSONEncoder{},
		Decoder: JSONDecoder{},
	}
	for _, apply := range options {
		apply(opts)
	}
	dbname := fmt.Sprintf("%s.%s", opts.DBName, dbExtension)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Pallas{
		db:      db,
		Options: opts,
	}, nil
}

func (p *Pallas) Bucket(name string) *Filter {
	return NewFilter(p, name)
}

func (p *Pallas) NewBucket(name string) (*bbolt.Bucket, error) {
	tx, err := p.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	b, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	return b, err
}

func (p *Pallas) Drop(name string) error {
	dbname := fmt.Sprintf("%s.%s", name, dbExtension)
	return os.RemoveAll(dbname)
}

type DataEncoder interface {
	Encode(M) ([]byte, error)
}

type JSONEncoder struct{}

func (JSONEncoder) Encode(m M) ([]byte, error) {
	return json.Marshal(m)
}

type DataDecoder interface {
	Decode([]byte, any) error
}

type JSONDecoder struct{}

func (JSONDecoder) Decode(bytes []byte, a any) error {
	return json.Unmarshal(bytes, &a)
}

type Options struct {
	DBName  string
	Encoder DataEncoder
	Decoder DataDecoder
}

type Option func(opts *Options)

func WithDBName(name string) Option {
	return func(o *Options) {
		o.DBName = name
	}
}

func WithEncoder(encoder DataEncoder) Option {
	return func(o *Options) {
		o.Encoder = encoder
	}
}

func WithDecoder(decoder DataDecoder) Option {
	return func(o *Options) {
		o.Decoder = decoder
	}
}
