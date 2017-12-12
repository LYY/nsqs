package nsqs

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	nsq "github.com/nsqio/go-nsq"
)

// NsqStopable which has Stop() method for grancful stop, eg: nsq consumer/producer
type NsqStopable interface {
	Stop()
}

var (
	// ErrSimpleConfig is returned when simple config is not passed as parameter.
	ErrSimpleConfig = errors.New("simple config is mandatory")
)

var stopables []NsqStopable
var access sync.Mutex
var started bool
var wg sync.WaitGroup

// GlobalConfig global config
var GlobalConfig *SimpleConfig

// InitConfig initialize global emmiter
func InitConfig(conf *SimpleConfig) error {
	if conf == nil {
		return ErrSimpleConfig
	}
	GlobalConfig = conf
	return nil
}

// Run run server
func Run() {
	Start()
	go handleSignals()
	waitForExit()
}

// Start start nsqs
func Start() {
	access.Lock()
	defer access.Unlock()

	if started {
		return
	}
	started = true
}

func handleSignals() {
	quitSignal := make(chan os.Signal)
	signal.Notify(quitSignal, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)
	<-quitSignal

	quit()
}

// Quit quit server
func quit() {
	access.Lock()
	defer access.Unlock()

	if !started {
		return
	}
	started = false
	// common.Logger.Info("Stop nsqs ...")

	for _, ns := range stopables {
		go func(ns NsqStopable) {
			defer wg.Done()
			ns.Stop()
		}(ns)
	}
}

// Stop graceful stop server
func Stop() {
	quit()
	waitForExit()
}

// addNsqStopable Add a nsq consumer/producer
func addNsqStopable(ns NsqStopable) {
	access.Lock()
	defer access.Unlock()

	wg.Add(1)
	stopables = append(stopables, ns)
}

func waitForExit() {
	wg.Wait()
}

// HandlerFunc handler function
type HandlerFunc func(m *nsq.Message) error

// Register is register a topic listener
func Register(topic, channel string, handler HandlerFunc, concurrency int) (err error) {
	err = On(ListenerConfig{
		Lookup:             GlobalConfig.Lookups,
		Topic:              topic,
		Channel:            channel,
		HandlerConcurrency: concurrency,
	}, handleMessage(handler))
	return err
}

// RegisterDefault is register a topic listener with concurrency process and default configuration.
func RegisterDefault(topic string, handler HandlerFunc) (err error) {
	return Register(topic, "default", handler, 10)
}

// RegisterDefaultSerial is register a topic listener with serial process and default configuration.
func RegisterDefaultSerial(topic string, handler HandlerFunc) (err error) {
	return Register(topic, "default", handler, 1)
}

func handleMessage(handler HandlerFunc) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		err = recoverRunner(handler, message)
		return
	})
}

func recoverRunner(handler HandlerFunc, message *nsq.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				err = r
			default:
				err = fmt.Errorf("%v", r)
			}
			stack := make([]byte, 4<<10)
			length := runtime.Stack(stack, false)
			// if GlobalConfig.LogPanic {
			// common.Logger.Debugf("[%s] %s %s\n", color.Red("PANIC RECOVER"), err, stack[:length])
			// }
		}
	}()
	err = handler(message)
	return
}

// PostTopic post topic
func PostTopic(topic string, payload interface{}) (err error) {
	err = ShootMessage(GlobalConfig.NsqAddress, topic, payload)
	// common.Logger.Debug("nsqs post topic [", topic, "] playload: [", payload, "] error:", err)
	return
}
