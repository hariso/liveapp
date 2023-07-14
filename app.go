package main

import (
	// Dependencies of the example data app
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"

	// Dependencies of Turbine
	"github.com/meroxa/turbine-go/v2/pkg/turbine"
	"github.com/meroxa/turbine-go/v2/pkg/turbine/cmd"
)

func main() {
	cmd.Start(App{})
}

var _ turbine.App = (*App)(nil)

type App struct{}

func (a App) Run(v turbine.Turbine) error {
	source, err := v.Resources("haris_pg_db")
	if err != nil {
		return err
	}

	rr, err := source.Records("products", []turbine.ConnectionOption{
		{
			Field: "url",
			Value: "postgres://meroxa:d1254d9ac6bda4b33b590aa14a@diffuser-production-01h57f86mk04zpn8aqs16myx41.cg05juyjuelr.us-east-1.rds.amazonaws.com:5432/diffuser",
		},
		{
			Field: "table",
			Value: "products",
		},
	})
	if err != nil {
		return err
	}

	dest, err := v.Resources("haris_pg_db")
	if err != nil {
		return err
	}

	res, err := v.Process(rr, Anonymize{})
	if err != nil {
		return err
	}

	err = dest.WriteWithConfig(
		res,
		"products_enriched",
		[]turbine.ConnectionOption{
			{
				Field: "url",
				Value: "postgres://meroxa:d1254d9ac6bda4b33b590aa14a@diffuser-production-01h57f86mk04zpn8aqs16myx41.cg05juyjuelr.us-east-1.rds.amazonaws.com:5432/diffuser",
			},
			{
				Field: "table",
				Value: "products_enriched",
			},
		},
	)
	if err != nil {
		return err
	}

	return nil
}

type NoOp struct {
}

func (n NoOp) Process(stream []turbine.Record) []turbine.Record {
	log.Println("NoOp Process called.")
	return stream
}

type Anonymize struct{}

func (f Anonymize) Process(stream []turbine.Record) []turbine.Record {
	log.Println("inside Anonymize.Process()")
	for i, record := range stream {
		email := fmt.Sprintf("%s", record.Payload.Get("customer_email"))
		if email == "" {
			log.Printf("unable to find customer_email value in record %d\n", i)
			break
		}
		hashedEmail := consistentHash(email)
		err := record.Payload.Set("customer_email", hashedEmail)
		if err != nil {
			log.Println("error setting value: ", err)
			continue
		}
		stream[i] = record
	}
	return stream
}

func consistentHash(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}
