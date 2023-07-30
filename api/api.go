package api

import (
	"encoding/json"
	"fmt"
	"github.com/Nicknamezz00/pallas/pallas"
	"github.com/labstack/echo/v4"
	"net/http"
	"strings"
)

type Server struct {
	db *pallas.Pallas
}

func NewServer(db *pallas.Pallas) *Server {
	return &Server{
		db: db,
	}
}

// PostHandler handle set kv requests.
func (s *Server) PostHandler(c echo.Context) error {
	var bucketName = c.Param("b")
	var v pallas.M
	if err := json.NewDecoder(c.Request().Body).Decode(&v); err != nil {
		return err
	}
	id, err := s.db.Bucket(bucketName).Insert(v)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, pallas.M{"id": id})
}

// GetHandler handle query value requests.
func (s *Server) GetHandler(c echo.Context) error {
	var (
		bucketName = c.Param("b")
		filterMap  = NewFilterMap()
	)
	for k, v := range c.QueryParams() {
		sep := strings.Split(k, ".")
		if len(sep) != 2 || len(sep) == 0 {
			return fmt.Errorf("invalid query")
		}
		if sep[0] == "" {
			return fmt.Errorf("invalid query")
		}
		var (
			ftype  = sep[0]
			fkey   = sep[1]
			fvalue = v[0]
		)
		filterMap.Add(ftype, fkey, fvalue)
	}
	records, err := s.db.Bucket(bucketName).Equal(filterMap.Get(pallas.COND_EQ)).Find()
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, records)
}

