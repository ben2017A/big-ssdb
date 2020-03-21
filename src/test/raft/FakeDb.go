package main

import (
	"log"
)

type FakeDb struct {
	mm map[string]string
}

func NewFakeDb() *FakeDb {
	db := new(FakeDb)
	db.mm = make(map[string]string)
	return db
}

func (db *FakeDb)Close(){
	log.Println("Close")
}

func (db *FakeDb)All() map[string]string {
	return db.mm
}

func (db *FakeDb)Get(key string) string {
	return db.mm[key]
}

func (db *FakeDb)Set(key string, val string) {
	db.mm[key] = val
}

func (db *FakeDb)CleanAll(){
	db.mm = make(map[string]string)
}

func (db *FakeDb)Fsync() error {
	return nil
}
