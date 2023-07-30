/*
 * MIT License
 *
 * Copyright (c) 2023 Runze Wu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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
