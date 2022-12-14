package main

import (
	"encoding/json"
	"log"

	// Dependencies of Turbine
	"github.com/meroxa/turbine-go"
	"github.com/meroxa/turbine-go/runner"
)

func main() {
	runner.Start(App{})
}

var _ turbine.App = (*App)(nil)

type App struct{}

func (a App) Run(v turbine.Turbine) error {
	// To configure your data stores as resources on the Meroxa Platform
	// use the Meroxa Dashboard, CLI, or Meroxa Terraform Provider.
	// For more details refer to: https://docs.meroxa.com/
	//
	// Identify an upstream data store for your data app
	// with the `Resources` function
	// Replace `source_name` with the resource name the
	// data store was configured with on Meroxa.

	source, err := v.Resources("mongodb-resource-two")
	if err != nil {
		return err
	}

	// Specify which upstream records to pull
	// with the `Records` function
	// Replace `collection_name` with a table, collection,
	// or bucket name in your data store.
	// If a configuration is needed for your source,
	// you can pass it as a second argument to the `Records` function. For example:
	//
	// source.Records("collection_name", turbine.ResourceConfigs{turbine.ResourceConfig{Field: "incrementing.field.name", Value:"id"}})

	rr, err := source.Records(
		"users",
		nil,
	)
	if err != nil {
		return err
	}

	// Specify what code to execute against upstream records
	// with the `Process` function
	// Replace `Anonymize` with the name of your function code.

	res := v.Process(rr, Anonymize{})

	// Identify a downstream data store for your data app
	// with the `Resources` function
	// Replace `destination_name` with the resource name the
	// data store was configured with on Meroxa.

	dest, err := v.Resources("mongodb-resource-two")
	if err != nil {
		return err
	}

	// Specify where to write records downstream
	// using the `Write` function
	// Replace `collection_archive` with a table, collection,
	// or bucket name in your data store.
	// If a configuration is needed, you can also use i.e.
	//
	// dest.WriteWithConfig(
	//  res,
	//  "my-archive",
	//  turbine.ResourceConfigs{turbine.ResourceConfig{Field: "buffer.flush.time", Value: "10"}}
	// )

	err = dest.WriteWithConfig(
		res,
		"users_archive",
		turbine.ConnectionOptions{
			turbine.ConnectionOption{
				Field: "transforms",
				Value: `RenameField`,
			},
			turbine.ConnectionOption{
				Field: "transforms.RenameField.type",
				Value: `org.apache.kafka.connect.transforms.ReplaceField$Value`,
			},
			turbine.ConnectionOption{
				Field: "transforms.RenameField.renames",
				Value: `source:debezium_source`,
			},
		},
	)

	if err != nil {
		return err
	}

	return nil
}

type Anonymize struct{}

func (f Anonymize) Process(stream []turbine.Record) []turbine.Record {
	return stream
}

func (f Anonymize) ProcessOld(stream []turbine.Record) []turbine.Record {
	for i, record := range stream {
		afterS := record.Payload.Get("after").(string)
		var after map[string]interface{}
		err := json.Unmarshal([]byte(afterS), &after)
		if err != nil {
			log.Printf("got unmarshal error %v", err)
		}
		log.Printf("got after value: %+v\n", after)

		after["processed_by"] = "haris-turbine-app"

		bytes, err := json.Marshal(after)
		if err != nil {
			log.Printf("got marshal error %v", err)
		}
		record.Payload = bytes
		stream[i] = record
	}
	return stream
}
