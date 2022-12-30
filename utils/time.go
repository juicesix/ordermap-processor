package utils

import (
	"fmt"
	"time"

	"github.com/juicesix/logging"
)

func TimeCost(start time.Time, execute, name string) time.Duration {
	terminal := time.Since(start)
	//fmt.Println(fmt.Sprintf("%v-%v 方法耗时:%v", execute, name, terminal))
	logging.Debugf(fmt.Sprintf("%v-%v 方法耗时:%v", execute, name, terminal))
	return terminal
}
