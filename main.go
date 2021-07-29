package main

import (
	"fmt"
	"github.com/kardianos/service"
	"sync"
	"time"
)

var (
	serviceIsRunning bool
	programIsRunning bool
	writingSync      sync.Mutex
)

const serviceName = "benchmark service"
const serviceDescription = "benchmark service"

type program struct{}

func (p program) Start(s service.Service) error {
	fmt.Println(s.String() + " started")
	writingSync.Lock()
	serviceIsRunning = true
	writingSync.Unlock()
	go p.run()
	return nil
}

func (p program) Stop(s service.Service) error {
	writingSync.Lock()
	serviceIsRunning = false
	writingSync.Unlock()
	for programIsRunning {
		fmt.Println(s.String() + " stopping...")
		time.Sleep(1 * time.Second)
	}
	fmt.Println(s.String() + " stopped")
	return nil
}

func (p program) run() {
	writingSync.Lock()
	programIsRunning = true
	writingSync.Unlock()
	for i := 0; i < 5; i++ {
		if serviceIsRunning {
			//writeBenchmark("postgres")
			//writeBenchmark("timescale")
			writeBenchmark("mariadb-native")
			writeBenchmark("mariadb-docker")
			//writeBenchmark("percona")
			//writeBenchmark("sqlserver")
			time.Sleep(1 * time.Second)
		}
	}
	for i := 0; i < 5; i++ {
		if serviceIsRunning {
			//readBenchmark("postgres")
			//readBenchmark("timescale")
			readBenchmark("mariadb-native")
			readBenchmark("mariadb-docker")
			//readBenchmark("percona")
			//readBenchmark("sqlserver")
			time.Sleep(1 * time.Second)
		}
	}
	writingSync.Lock()
	programIsRunning = false
	writingSync.Unlock()
}

func main() {
	serviceConfig := &service.Config{
		Name:        serviceName,
		DisplayName: serviceName,
		Description: serviceDescription,
	}
	prg := &program{}
	s, err := service.New(prg, serviceConfig)
	if err != nil {
		fmt.Println("Cannot create the service: " + err.Error())
	}
	err = s.Run()
	if err != nil {
		fmt.Println("Cannot start the service: " + err.Error())
	}
}
