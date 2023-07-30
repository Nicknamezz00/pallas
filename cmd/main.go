package main

import (
	"fmt"
	"github.com/Nicknamezz00/pallas/api"
	"github.com/Nicknamezz00/pallas/pallas"
	"github.com/labstack/echo/v4"
	"log"
	"net/http"
)

func main() {
	db, err := pallas.NewPallas()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(db.DBName)
	srv := api.NewServer(db)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		_ = c.JSON(http.StatusInternalServerError, pallas.M{"error": err.Error()})
	}
	e.POST("/api/v1:b", srv.PostHandler)
	e.GET("/api/v1:b", srv.GetHandler)
	log.Fatal(e.Start(":5100"))
}
