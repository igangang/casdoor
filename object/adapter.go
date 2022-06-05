// Copyright 2021 The Casdoor Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package object

import (
	"fmt"
	"runtime"

	"github.com/astaxie/beego"
	"github.com/casdoor/casdoor/conf"
	"github.com/casdoor/casdoor/util"
	//_ "github.com/denisenkom/go-mssqldb" // db = mssql
	_ "github.com/go-sql-driver/mysql" // db = mysql
	//_ "github.com/lib/pq"                // db = postgres
	"xorm.io/core"
	"xorm.io/xorm"
)

var adapter *Adapter

func InitConfig() {
	err := beego.LoadAppConfig("ini", "../conf/app.conf")
	if err != nil {
		panic(err)
	}

	InitAdapter(true)
}

func InitAdapter(createDatabase bool) {

	adapter = NewAdapter(conf.GetConfigString("driverName"), conf.GetBeegoConfDataSourceName(), conf.GetConfigString("dbName"))
	if createDatabase {
		adapter.CreateDatabase()
	}
	adapter.createTable()
}

// Adapter represents the MySQL adapter for policy storage.
type Adapter struct {
	driverName     string
	dataSourceName string
	dbName         string
	Engine         *xorm.Engine
}

// finalizer is the destructor for Adapter.
func finalizer(a *Adapter) {
	err := a.Engine.Close()
	if err != nil {
		panic(err)
	}
}

// NewAdapter is the constructor for Adapter.
func NewAdapter(driverName string, dataSourceName string, dbName string) *Adapter {
	a := &Adapter{}
	a.driverName = driverName
	a.dataSourceName = dataSourceName
	a.dbName = dbName

	// Open the DB, create it if not existed.
	a.open()

	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)

	return a
}

func (a *Adapter) CreateDatabase() error {
	engine, err := xorm.NewEngine(a.driverName, a.dataSourceName)
	if err != nil {
		return err
	}
	defer engine.Close()

	_, err = engine.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s default charset utf8mb4 COLLATE utf8mb4_general_ci", a.dbName))
	return err
}

func (a *Adapter) open() {
	dataSourceName := a.dataSourceName + a.dbName
	if a.driverName != "mysql" {
		dataSourceName = a.dataSourceName
	}

	engine, err := xorm.NewEngine(a.driverName, dataSourceName)
	if err != nil {
		panic(err)
	}

	a.Engine = engine
}

func (a *Adapter) close() {
	_ = a.Engine.Close()
	a.Engine = nil
}

func (a *Adapter) createTable() {
	showSql, _ := conf.GetConfigBool("showSql")
	a.Engine.ShowSQL(showSql)

	tableNamePrefix := conf.GetConfigString("tableNamePrefix")
	tbMapper := core.NewPrefixMapper(core.SnakeMapper{}, tableNamePrefix)
	a.Engine.SetTableMapper(tbMapper)

	organization := new(Organization)
	res, err := a.Engine.IsTableExist(organization)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(organization)
		if err != nil {
			panic(err)
		}
	}
	
	user := new(User)
	res, err = a.Engine.IsTableExist(user)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(user)
		if err != nil {
			panic(err)
		}
	}

	role := new(Role)
	res, err = a.Engine.IsTableExist(role)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(role)
		if err != nil {
			panic(err)
		}
	}

	permission := new(Permission)
	res, err = a.Engine.IsTableExist(permission)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(permission)
		if err != nil {
			panic(err)
		}
	}

	model := new(Model)
	res, err = a.Engine.IsTableExist(model)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(model)
		if err != nil {
			panic(err)
		}
	}

	provider := new(Provider)
	res, err = a.Engine.IsTableExist(provider)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(provider)
		if err != nil {
			panic(err)
		}
	}

	application := new(Application)
	res, err = a.Engine.IsTableExist(application)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(application)
		if err != nil {
			panic(err)
		}
	}

	resource := new(Resource)
	res, err = a.Engine.IsTableExist(resource)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(resource)
		if err != nil {
			panic(err)
		}
	}

	token := new(Token)
	res, err = a.Engine.IsTableExist(token)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(token)
		if err != nil {
			panic(err)
		}
	}

	verificationRecord := new(VerificationRecord)
	res, err = a.Engine.IsTableExist(verificationRecord)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(verificationRecord)
		if err != nil {
			panic(err)
		}
	}

	record := new(Record)
	res, err = a.Engine.IsTableExist(record)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(record)
		if err != nil {
			panic(err)
		}
	}

	webhook := new(Webhook)
	res, err = a.Engine.IsTableExist(webhook)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(webhook)
		if err != nil {
			panic(err)
		}
	}

	syncer := new(Syncer)
	res, err = a.Engine.IsTableExist(syncer)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(syncer)
		if err != nil {
			panic(err)
		}
	}

	cert := new(Cert)
	res, err = a.Engine.IsTableExist(cert)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(cert)
		if err != nil {
			panic(err)
		}
	}

	product := new(Product)
	res, err = a.Engine.IsTableExist(product)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(product)
		if err != nil {
			panic(err)
		}
	}

	payment := new(Payment)
	res, err = a.Engine.IsTableExist(payment)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(payment)
		if err != nil {
			panic(err)
		}
	}

	ldap := new(Ldap)
	res, err = a.Engine.IsTableExist(ldap)
	if err != nil {
		panic(err)
	}
	if !res {
		err = a.Engine.Sync2(ldap)
		if err != nil {
			panic(err)
		}
	}
}

func GetSession(owner string, offset, limit int, field, value, sortField, sortOrder string) *xorm.Session {
	session := adapter.Engine.Prepare()
	if offset != -1 && limit != -1 {
		session.Limit(limit, offset)
	}
	if owner != "" {
		session = session.And("owner=?", owner)
	}
	if field != "" && value != "" {
		if filterField(field) {
			session = session.And(fmt.Sprintf("%s like ?", util.SnakeString(field)), fmt.Sprintf("%%%s%%", value))
		}
	}
	if sortField == "" || sortOrder == "" {
		sortField = "created_time"
	}
	if sortOrder == "ascend" {
		session = session.Asc(util.SnakeString(sortField))
	} else {
		session = session.Desc(util.SnakeString(sortField))
	}
	return session
}
