package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	SourceHosts        = flag.String("SourceHosts", os.Getenv("SourceHosts"), "MongoDBHosts")
	SourceAuthDatabase = flag.String("SourceAuthDatabase", os.Getenv("SourceAuthDatabase"), "AuthDatabase ")
	SourceAuthPassword = flag.String("SourceAuthPassword", os.Getenv("SourceAuthPassword"), "AuthPassword ")
	SourceAuthUserName = flag.String("SourceAuthUserName", os.Getenv("SourceAuthUserName"), "AuthUserName ")
	SourceDatabase     = flag.String("SourceDatabase", os.Getenv("SourceDatabase"), "Database ")
	SourceCollection   = flag.String("SourceCollection", os.Getenv("SourceCollection"), "segments")
	DestHosts          = flag.String("DestHosts", os.Getenv("DestHosts"), "MongoDBHosts")
	DestAuthDatabase   = flag.String("DestAuthDatabase", os.Getenv("DestAuthDatabase"), "AuthDatabase ")
	DestAuthPassword   = flag.String("DestAuthPassword", os.Getenv("DestAuthPassword"), "AuthPassword ")
	DestAuthUserName   = flag.String("DestAuthUserName", os.Getenv("DestAuthUserName"), "AuthUserName ")
	DestDatabase       = flag.String("DestDatabase", os.Getenv("DestDatabase"), "Database ")
	DestCollection     = flag.String("DestCollection", os.Getenv("DestCollection"), "segments")
)

func setUpMongoConnection(addrs []string, authDatabase, userName, pass, collection string) *mgo.Session {
	config := &mgo.DialInfo{
		Addrs:    addrs,
		Username: userName,
		Password: pass,
		Database: authDatabase,
	}
	mongoSession, err := mgo.DialWithInfo(config)
	if err != nil {
		panic("CreateSession error:" + err.Error())
	}
	mongoSession.SetMode(mgo.Monotonic, true)
	return mongoSession

}

func main() {
	flag.Parse()
	fmt.Printf("SourceHosts =%s\n", *SourceHosts)
	fmt.Printf("SourceAuthDatabase =%s\n", *SourceAuthDatabase)
	fmt.Printf("SourceAuthUserName =%s\n", *SourceAuthUserName)
	fmt.Printf("SourceAuthUserName =%s\n", *SourceAuthPassword)
	fmt.Printf("SourceCollection =%s\n", *SourceCollection)
	fmt.Printf("SourceDatabase =%s\n", *SourceDatabase)
	fmt.Printf("\n------------\n\n\n-----------\n")
	fmt.Printf("DestHosts =%s\n", *DestHosts)
	fmt.Printf("DestAuthDatabase =%s\n", *DestAuthDatabase)
	fmt.Printf("DestAuthUserName =%s\n", *DestAuthUserName)
	fmt.Printf("DestAuthUserName =%s\n", *DestAuthPassword)
	fmt.Printf("DestCollection =%s\n", *DestCollection)
	fmt.Printf("DestDatabase =%s\n", *DestDatabase)

	sourcesession := setUpMongoConnection(strings.Split(*SourceHosts, ","), *SourceAuthDatabase, *SourceAuthUserName, *SourceAuthPassword, *SourceCollection)
	copySs := sourcesession.Copy()
	sc := copySs.DB(*SourceDatabase).C(*SourceCollection)
	iter := sc.Find(nil).Iter()

	destsession := setUpMongoConnection(strings.Split(*DestHosts, ","), *DestAuthDatabase, *DestAuthUserName, *DestAuthPassword, *DestCollection)
	destS := destsession.Copy()
	dc := destS.DB(*DestDatabase).C(*DestCollection)

	var result bson.M
	n := 0
	for iter.Next(&result) {
		_id := result["_id"]
		if ci, err := dc.UpsertId(_id, result); err != nil {
			fmt.Println(err.Error())
		} else {
			n = n + ci.Updated
			//fmt.Printf("ci.Updated %d ,ci.UpsertedId %d \n", ci.Updated, ci.UpsertedId)
		}

	}

	fmt.Printf("Copied %d Records", n)

}
