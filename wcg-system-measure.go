package main

import (
	"flag"
	//"fmt"
	"github.com/dwdcth/consoleEx"
	"github.com/garyburd/redigo/redis"
	"github.com/mattn/go-colorable"
	"github.com/rs/zerolog"
	//"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	// "os"
	// "reflect"
	"strconv"
	"strings"
	//"sync"
	"time"
)

const (
	REDIS_ADDR     string = "127.0.0.1:9736"
	REDIS_PROTOCOL string = "tcp"
)

const (
	RedisKeyReq  string = "test2"
	RedisKeyResp string = "dongfeng"
)

type OperationType string

const (
	OperationType_ADD    OperationType = "0"
	OperationType_DELETE OperationType = "1"
	OperationType_HANGUP OperationType = "2"
	OperationType_WAKEUP OperationType = "3"
	OperationType_QUERY  OperationType = "4"
)

type SystemInfoType string

const (
	SystemInfoType_MEM  SystemInfoType = "32"
	SystemInfoType_CPU  SystemInfoType = "33"
	SystemInfoType_DISK SystemInfoType = "34"
)

type Task struct {
	TaskId         string
	OperationType  OperationType
	TimeInterval   string
	TimeStart      string
	TimeEnd        string
	SystemInfoType []SystemInfoType
	Delete         chan struct{}
	HangUp         chan struct{}
}

var (
	cronlist []Task
	swplist  []Task
)

//var mutex = sync.Mutex{}
//var cond = sync.NewCond(&mutex)

var logger zerolog.Logger

func ConnRedis() redis.Conn {
	conn, err := redis.Dial(REDIS_PROTOCOL, REDIS_ADDR)
	if err != nil {
		logger.Error().Err(err)
	}
	return conn
}

func Push(message string) { //发布者
	conn := ConnRedis()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", "channel2", message)
	if err != nil {
		logger.Error().Err(err)
		return
	}
}

func Subs() { //订阅者
	conn := ConnRedis()
	defer conn.Close()
	psc := redis.PubSubConn{conn}
	psc.Subscribe("channel1") //订阅channel1频道
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			logger.Debug().Msgf("%s: message: %s", v.Channel, v.Data)
			go HandleTask()
		case redis.Subscription:
			logger.Debug().Msgf("%s: %s %d", v.Channel, v.Kind, v.Count)
		case error:
			logger.Error().Err(v)
			return
		}
	}
}

//统计系统信息
func Sysinfo(sit SystemInfoType) float64 {
	switch sit {
	case SystemInfoType_MEM:
		v, _ := mem.VirtualMemory()
		logger.Info().Msgf("Mem: %v MB  Free: %v MB Used:%v Usage:%f%%", v.Total/1024/1024, v.Available/1024/1024, v.Used/1024/1024, v.UsedPercent)
		return v.UsedPercent
	case SystemInfoType_CPU:
		c, _ := cpu.Percent(0, false)
		logger.Info().Msgf("CPU Used: used %f%% ", c[0])
		return c[0]
	case SystemInfoType_DISK:
		d, _ := disk.Usage("/")
		logger.Info().Msgf("HD: %v GB  Free: %v GB Usage:%f%%", d.Total/1024/1024/1024, d.Free/1024/1024/1024, d.UsedPercent)
		return d.UsedPercent
	}
	return 0
}

//上报系统信息
func Update(task Task, count int) {
	conn := ConnRedis()
	defer conn.Close()
	conn.Do("hset", RedisKeyResp, "taskid", task.TaskId)
	conn.Do("hset", RedisKeyResp, "time", time.Now().Unix())
	conn.Do("hset", RedisKeyResp, "statisticstimes", count)
	for _, sit := range task.SystemInfoType {
		switch sit {
		case SystemInfoType_MEM:
			conn.Do("hset", RedisKeyResp, SystemInfoType_MEM, Sysinfo(sit))
		case SystemInfoType_CPU:
			conn.Do("hset", RedisKeyResp, SystemInfoType_CPU, Sysinfo(sit))
		case SystemInfoType_DISK:
			conn.Do("hset", RedisKeyResp, SystemInfoType_DISK, Sysinfo(sit))
		}
	}
}

//获取请求
func NewTask() Task {
	var task Task
	conn := ConnRedis()
	defer conn.Close()
	taskid, _ := redis.String(conn.Do("hget", RedisKeyReq, "taskid"))
	_operationtype, _ := redis.String(conn.Do("hget", RedisKeyReq, "operationtype"))
	operationtype := OperationType(_operationtype)
	//重复操作判断
	for _, v := range cronlist {
		if v.TaskId == taskid && v.OperationType == operationtype {
			logger.Error().Msg("the same taskid")
			Push("error!")
			logger.Debug().Msg("push error to redis")
			return Task{}
		}
	}
	//如果是add则获取更多信息
	switch operationtype {
	case OperationType_ADD:
		_concerninfo, _ := redis.String(conn.Do("hget", RedisKeyReq, "concerninfo"))
		concerninfo := strings.Split(_concerninfo, " ")
		for _, _v := range concerninfo {
			v := strings.Split(_v, ":")
			if v[0] == "SYSTEM" {
				//获取SystemInfoType
				vv := strings.Split(v[1], ",")
				for _, vvv := range vv {
					task.SystemInfoType = append(task.SystemInfoType, SystemInfoType(vvv))
				}
				task.TaskId = taskid
				task.OperationType = operationtype
				task.TimeInterval, _ = redis.String(conn.Do("hget", RedisKeyReq, "timeinterval"))
				task.TimeStart, _ = redis.String(conn.Do("hget", RedisKeyReq, "timestart"))
				task.TimeEnd, _ = redis.String(conn.Do("hget", RedisKeyReq, "timeend"))
				task.Delete = make(chan struct{})
				task.HangUp = make(chan struct{})
				return task
			}
		}
	default:
		return Task{TaskId: taskid, OperationType: operationtype}
	}
	return Task{}
}

func OneCron(task Task) {
	count := 1
	interval, _ := strconv.ParseInt(task.TimeInterval, 10, 64)
	timer := time.NewTimer(time.Second * 1)
	start, _ := strconv.ParseInt(task.TimeStart, 10, 64)
	end, _ := strconv.ParseInt(task.TimeEnd, 10, 64)

	for {
		select {
		case <-task.Delete:
			return
		case <-task.HangUp:
			return
		case <-timer.C:
			if time.Now().Unix() >= start && time.Now().Unix() <= end {
				logger.Debug().Msgf("[taskid]%s", task.TaskId)
				Update(task, count)
				logger.Debug().Msgf("count is %d", count)
				count++
				Push("egwKPIResponse")
				logger.Debug().Msg("push egwKPIResponse to redis")
				timer.Reset(time.Duration(interval) * 1000 * 1000 * 1000)
			}
		}
	}
}

func HandleTask() {
	task := NewTask()
	switch task.OperationType {
	case OperationType_ADD:
		logger.Debug().Msg("OperationType_ADD")
		cronlist = append(cronlist, task)
		go OneCron(task)
		logger.Debug().Msgf("add %s", task.TaskId)
	case OperationType_DELETE:
		logger.Debug().Msg("OperationType_DELETE")
		for i, v := range cronlist {
			if v.TaskId == task.TaskId {
				cronlist = append(cronlist[:i], cronlist[i+1:]...)
				v.Delete <- struct{}{}
				logger.Debug().Msgf("delete %s", task.TaskId)
			}
		}
		for i, v := range swplist {
			if v.TaskId == task.TaskId {
				swplist = append(swplist[:i], swplist[i+1:]...)
				logger.Debug().Msgf("delete %s", task.TaskId)
			}
		}
	case OperationType_HANGUP:
		logger.Debug().Msg("OperationType_HANGUP")
		for i, v := range cronlist {
			if v.TaskId == task.TaskId {
				swplist = append(swplist, v)
				cronlist = append(cronlist[:i], cronlist[i+1:]...)
				v.HangUp <- struct{}{}
				logger.Debug().Msgf("hangup %s", task.TaskId)
			}
		}
	case OperationType_WAKEUP:
		logger.Debug().Msg("OperationType_WAKEUP")
		for i, v := range swplist {
			if v.TaskId == task.TaskId {
				cronlist = append(cronlist, v)
				swplist = append(swplist[:i], swplist[i+1:]...)
				go OneCron(v)
				logger.Debug().Msgf("wakeup %s", task.TaskId)
			}
		}
	case OperationType_QUERY:
	}
}

func LogSet() {
	out := consoleEx.ConsoleWriterEx{Out: colorable.NewColorableStdout()}
	logger = zerolog.New(out).With().Caller().Timestamp().Logger()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	debug := flag.Bool("debug", false, "sets log level to debug")
	info := flag.Bool("info", false, "set log level to info")
	flag.Parse()
	switch {
	case *debug:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case *info:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	zerolog.CallerSkipFrameCount = 2
}

func main() {
	LogSet()
	Subs()
}
