package api

import (
	"github.com/Nicknamezz00/pallas/pallas"
	"strconv"
)

type FilterMap struct {
	filters map[string]pallas.M
}

func NewFilterMap() *FilterMap {
	filters := make(map[string]pallas.M)
	filters[pallas.COND_EQ] = pallas.M{}
	return &FilterMap{
		filters: filters,
	}
}

func (f *FilterMap) Get(filterType string) pallas.M {
	v, ok := f.filters[filterType]
	if !ok {
		return pallas.M{}
	}
	return v
}

func (f *FilterMap) Add(filterType, k, v string) {
	if _, ok := f.filters[filterType]; !ok {
		return
	}
	f.filters[filterType][k] = safeConvertString(v)
}

func safeConvertString(v string) any {
	switch {
	case v == "true":
		return true
	case v == "false":
		return false
	case isInt(v):
		val, _ := strconv.Atoi(v)
		return val
	case isFloat(v):
		val, _ := strconv.ParseFloat(v, 64)
		return val
	default:
		return v
	}
}

func isInt(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func isFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
