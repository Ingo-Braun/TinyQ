package hooks

import (
	"context"
	"errors"
	"log"
	"reflect"
	"runtime"
	"sync"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
)

// maximum hooks allowed
const MaxHooksCount int = 10

// error raised when attempting to add a new hook when the maximum amount of hooks would be exceeded
var ErrorMaxHooks error = errors.New("maximum hooks allowed error")

// error raised when attempting to start the worker routine when not in routine mode
var ErrorRoutineModeDisabled error = errors.New("routine mode is disabled error")

// error raised when executing an hook returns an error
var ErrorExecutingHooks error = errors.New("failed executing hooks")

type Hook func(message *Messages.RouterMessage) error
type HookChannel chan *Messages.RouterMessage

type HookExecutor struct {
	hooks                []Hook
	hooksCount           int
	hooksFailed          int64
	inputChannel         chan *Messages.RouterMessage
	hooksMutex           sync.Mutex
	routineMode          bool
	executorContext      context.Context
	executorStopFunction context.CancelFunc
}

// creates new hook executor
func CreateHookExecutor(routineMode bool) *HookExecutor {
	executor := HookExecutor{
		inputChannel: make(chan *Messages.RouterMessage, 10),
		routineMode:  routineMode,
	}
	return &executor
}

// add an hook to the executor processing queue
// returns ErrorMaxHooks when maximum hooks count is exceeded
func (hookExecutor *HookExecutor) AddHook(hook Hook) error {
	hookExecutor.hooksMutex.Lock()
	defer hookExecutor.hooksMutex.Unlock()
	if hookExecutor.hooksCount < MaxHooksCount {
		hookExecutor.hooks = append(hookExecutor.hooks, hook)
		hookExecutor.hooksCount++
		return nil
	}
	return ErrorMaxHooks
}

// returns the count of hooks in the processing queue
func (hookExecutor *HookExecutor) Count() int {
	hookExecutor.hooksMutex.Lock()
	defer hookExecutor.hooksMutex.Unlock()
	return hookExecutor.hooksCount
}

// returns the number of times hooks failed executing
func (hookExecutor *HookExecutor) FailedCount() int64 {
	hookExecutor.hooksMutex.Lock()
	defer hookExecutor.hooksMutex.Unlock()
	return hookExecutor.hooksFailed
}

// start the hook executor routine
// return ErrorRoutineModeDisabled if routine mode is disabled
func (hookExecutor *HookExecutor) StartExecutor() error {
	hookExecutor.hooksMutex.Lock()
	defer hookExecutor.hooksMutex.Unlock()
	if !hookExecutor.routineMode {
		return ErrorRoutineModeDisabled
	}
	hookExecutor.executorContext, hookExecutor.executorStopFunction = context.WithCancel(context.Background())
	go hookExecutor.executor()
	return nil
}

// main hook executor
// used on routine mode
func (hookExecutor *HookExecutor) executor() {
	for {
		select {
		case <-hookExecutor.executorContext.Done():
			return
		case msg := <-hookExecutor.inputChannel:
			hook, err := hookExecutor.Execute(msg)
			if err != nil {
				log.Printf("hook %v is failing", getHookName(*hook))
				hookExecutor.hooksMutex.Lock()
				defer hookExecutor.hooksMutex.Unlock()
				hookExecutor.hooksFailed++
			}
		}
	}
}

// returns the messages input channel witch is used by the hook executor
func (hookExecutor *HookExecutor) GetInputChannel() chan *Messages.RouterMessage {
	return hookExecutor.inputChannel
}

// hooks queue executor
// executes all queue with passed message
func (hookExecutor *HookExecutor) Execute(message *Messages.RouterMessage) (*Hook, error) {
	hookExecutor.hooksMutex.Lock()
	defer hookExecutor.hooksMutex.Unlock()
	for hookIndex := range hookExecutor.hooks {
		hook := hookExecutor.hooks[hookIndex]
		err := hook(message)
		if err != nil {
			log.Printf("failed executing hook %v with error %v \n", getHookName(hook), err)
			return &hook, err
		}
	}
	return nil, nil
}

// stops the executor routine
func (hookExecutor *HookExecutor) Stop() error {
	hookExecutor.executorStopFunction()
	return nil
}

// internal use returns the name of the function passed as hook
// used to log witch hook failed executing
func getHookName(hook Hook) string {
	return runtime.FuncForPC(reflect.ValueOf(hook).Pointer()).Name()
}
