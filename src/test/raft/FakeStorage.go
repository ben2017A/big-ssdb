package main

import (
	"log"
)

type FakeStorage struct {
	mm map[string]string
}

func NewFakeStorage() *FakeStorage {
	db := new(FakeStorage)
	db.mm = make(map[string]string)
	return db
}

func (db *FakeStorage)Close(){
	log.Println("Close")
}

func (db *FakeStorage)All() map[string]string {
	return db.mm
}

func (db *FakeStorage)Get(key string) string {
	return db.mm[key]
}

func (db *FakeStorage)Set(key string, val string) {
	db.mm[key] = val
}

func (db *FakeStorage)CleanAll(){
	db.mm = make(map[string]string)
}
