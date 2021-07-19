package main

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

func readBenchmark(databaseType string) {
	start := time.Now()
	databases := createDatabasesMap()
	readData(databaseType, databases)
	fmt.Println(databaseType + ": read benchmarks took " + time.Since(start).String())
}

func readData(databaseType string, databases map[string]string) {
	var database *gorm.DB
	var err error
	switch databaseType {
	case "postgres":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "timescale":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mysql":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mariadb":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "percona":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "sqlserver":
		{
			database, err = gorm.Open(sqlserver.Open(databases[databaseType]), &gorm.Config{})
		}
	}
	sqlDB, _ := database.DB()
	defer sqlDB.Close()
	if err != nil {
		fmt.Println("Problem opening database: " + err.Error())
		return
	}
	for i := 0; i < 2000; i++ {
		var sum int
		_ = database.Table("benchmark_data").Select("sum(data)").Row().Scan(&sum)
		var avg int
		_ = database.Table("benchmark_data").Select("avg(data)").Row().Scan(&avg)
	}
}

func writeBenchmark(databaseType string) {
	start := time.Now()
	databases := createDatabasesMap()
	createDatabaseAndTable(databaseType, databases)
	writeData(databaseType, databases)
	fmt.Println(databaseType + ": write benchmarks took " + time.Since(start).String())
}

func createDatabasesMap() map[string]string {
	databases := make(map[string]string, 6)
	databases["postgres"] = "user=postgres password=password dbname=benchmark host=localhost port=5433 sslmode=disable"
	databases["mariadb"] = "root:password@tcp(localhost:3307)/benchmark?charset=utf8&parseTime=True&loc=Local"
	databases["mysql"] = "root:password@tcp(localhost:3306)/benchmark?charset=utf8&parseTime=True&loc=Local"
	databases["timescale"] = "user=postgres password=password dbname=benchmark host=localhost port=5434 sslmode=disable"
	databases["percona"] = "root:password@tcp(localhost:3308)/benchmark?charset=utf8&parseTime=True&loc=Local"
	databases["sqlserver"] = "sqlserver://sa:passw0rd.@localhost:1433?database=benchmark"
	return databases
}

func writeData(databaseType string, databases map[string]string) {
	var database *gorm.DB
	var err error
	switch databaseType {
	case "postgres":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "timescale":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mysql":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mariadb":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "percona":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "sqlserver":
		{
			database, err = gorm.Open(sqlserver.Open(databases[databaseType]), &gorm.Config{})
		}
	}
	sqlDB, _ := database.DB()
	defer sqlDB.Close()
	if err != nil {
		fmt.Println("Problem opening database: " + err.Error())
		return
	}
	for i := 0; i < 10000; i++ {
		var benchmarkData BenchmarkData
		benchmarkData.Time = time.Now()
		benchmarkData.Data = rand.Intn(100-0) + 0
		database.Save(&benchmarkData)
	}
}

type BenchmarkData struct {
	gorm.Model
	Data int
	Time time.Time
}

func createDatabaseAndTable(databaseType string, databases map[string]string) {
	var database *gorm.DB
	var err error
	switch databaseType {
	case "postgres":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "timescale":
		{
			database, err = gorm.Open(postgres.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mysql":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "mariadb":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "percona":
		{
			database, err = gorm.Open(mysql.Open(databases[databaseType]), &gorm.Config{})
		}
	case "sqlserver":
		{
			database, err = gorm.Open(sqlserver.Open(databases[databaseType]), &gorm.Config{})
		}
	}
	sqlDB, _ := database.DB()
	defer sqlDB.Close()
	if err != nil {
		fmt.Println("Problem opening database: " + err.Error())
		return
	}
	fmt.Println(databaseType + " connected")

	if !database.Migrator().HasTable(&BenchmarkData{}) {
		err := database.Migrator().CreateTable(&BenchmarkData{})
		if err != nil {
			fmt.Println("Cannot create table: " + err.Error())
			return
		}
		database.Raw("SELECT create_hypertable('benchmark_data', 'created_at');")
	} else {
		err := database.Migrator().AutoMigrate(&BenchmarkData{})
		if err != nil {
			fmt.Println("Cannot update table: " + err.Error())
			return
		}
	}
}
