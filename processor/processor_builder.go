package processor

import (
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/elliotchance/orderedmap"
	"github.com/juicesix/logging"
	"ordermap-processor/utils"
)

// Functor 函数对象
// 针对prefunc 返回错误则会终止后续的处理
// 可以定制返回结果，后续操作继续操作
type Functor func(funcAction *ProcessedData) (interface{}, error)

type ProcessorBuilder struct {
	mutex       sync.RWMutex // protects the file set
	name        string
	err         error
	processData *ProcessedData

	// 并发执行标记
	Concurrency int
	// 改为map用于去重
	downDispatchers map[Processor]int
	upDispatchers   map[Processor]int
	// 并发执行 map
	concurrencyMap *orderedmap.OrderedMap
	// 并发执行 map
	afterMap *orderedmap.OrderedMap
	// 并发执行 map
	rollbackMap *orderedmap.OrderedMap

	msgChan    chan interface{} // *item
	msgMaxSize int              // 最大缓存大小
	chanCount  uint32           // 当前协程数
	running    bool             // 是否正在运行
	// 预处理函数
	functorArray []Functor
	// 最后处理函数
	sufFunctor Functor
	wg         sync.WaitGroup
	logGuard   sync.RWMutex
}

func (d *ProcessorBuilder) receiveErr(ch chan error) {
	for err := range ch {
		d.err = err
		logging.Errorf(d.processData.LogHead+"ProcessorBuilder receiveErr,err:%v", err)
	}
}

func (d *ProcessorBuilder) Start() {
	defer d.Recover(d.rollbackMap)
	if d.running {
		return
	}
	defer utils.TimeCost(time.Now(), d.processData.LogHead+"Start", "ProcessorBuilder")
	logging.Debugf(d.processData.LogHead+"ProcessorBuilder start begin,,processData:%+v,", *d.processData)
	d.running = true
	d.executeStart(d.concurrencyMap)
	d.processData.Err = d.err

	// 执行回滚
	if d.err != nil {
		d.err = nil
		d.executeStart(d.rollbackMap)
	} else {
		// 执行后续操作
		d.executeStart(d.afterMap)
	}
	logging.Infof(d.processData.LogHead + "ProcessorBuilder start end")
}

func (d *ProcessorBuilder) Recover(rollbackMap *orderedmap.OrderedMap) {
	if r := recover(); r != nil {
		logging.Errorf(d.processData.LogHead+"ProcessorBuilder recover,err: %+v,processData:%+v,", d.err, *d.processData)
		// log stack
		var buf [4096 * 4]byte
		n := runtime.Stack(buf[:], false)
		logging.Errorf(d.processData.LogHead+"panic %+v,stack:%s ", r, string(buf[:n]))
		d.executeStart(rollbackMap)
	}
}

func (d *ProcessorBuilder) Init(data *ProcessedData) error {
	d.processData = data
	if d.msgMaxSize <= 0 {
		d.msgMaxSize = 1 // 无缓存
	}
	d.msgChan = make(chan interface{}, d.msgMaxSize)
	d.concurrencyMap = orderedmap.NewOrderedMap()
	d.afterMap = orderedmap.NewOrderedMap()
	d.rollbackMap = orderedmap.NewOrderedMap()
	d.wg = sync.WaitGroup{}
	d.downDispatchers = make(map[Processor]int)
	d.upDispatchers = make(map[Processor]int)
	d.chanCount = 0
	d.functorArray = nil
	d.sufFunctor = nil
	d.running = false
	return nil
}

func (d *ProcessorBuilder) executeStart(funcMap *orderedmap.OrderedMap) {
	if funcMap == nil || funcMap.Len() <= 0 {
		return
	}
	for _, mark := range funcMap.Keys() {
		concurrencyValue, _ := funcMap.Get(mark)
		functorArray := concurrencyValue.([]Functor)
		d.process(functorArray)
		funcMap.Delete(mark)
	}
}

func (d *ProcessorBuilder) IsRunning() bool {
	return d.running
}

func (d *ProcessorBuilder) SetName(name string) {
	d.name = name
}

func (d *ProcessorBuilder) GetName() string {
	return d.name
}

func (d *ProcessorBuilder) DownRegister(down Processor) {
	d.downDispatchers[down] = 1
	down.UpRegister(down)
}

func (d *ProcessorBuilder) UpRegister(up Processor) {
	d.upDispatchers[up] = 1
}

//SetPreFunc concurrencyMark 同一标记视为同一组进行并发处理
func (d *ProcessorBuilder) SetPreFunc(preFunctor Functor, concurrencyMark int) {
	if preFunctor == nil {
		return
	}
	d.setExecuteMap(preFunctor, concurrencyMark, d.concurrencyMap)
}

//SetPreFunc concurrencyMark 同一标记视为同一组进行并发处理
func (d *ProcessorBuilder) SetAfterFunc(preFunctor Functor, concurrencyMark int) {
	if preFunctor == nil {
		return
	}
	d.setExecuteMap(preFunctor, concurrencyMark, d.afterMap)
}

func (d *ProcessorBuilder) SetRollBackFuc(sufFunctor Functor, concurrencyMark int) {
	if sufFunctor == nil {
		return
	}
	d.setExecuteMap(sufFunctor, concurrencyMark, d.rollbackMap)
}

func (d *ProcessorBuilder) process(functorArray []Functor) {
	if d.err != nil {
		return
	}
	if len(functorArray) > 1 {
		d.groupProcess(functorArray)
		return
	}
	d.execute(functorArray[0])
}

func (d *ProcessorBuilder) groupProcess(functorArray []Functor) {
	d.wg.Add(len(functorArray))
	//d.logf(LogLevelInfo, " %dth process starting...\n", id)
	for _, functor := range functorArray {
		go d.groupExecute(functor)
	}
	d.wg.Wait()
	//for _, functor := range functorArray {
	//	select {
	//	case item, ok := <-d.msgChan:
	//		if !ok {
	//			break
	//		}
	//
	//		// 经过用户业务处理之后的返回值，可以简简单单返回item自身
	//		var ret interface{}
	//		var err error
	//
	//		// dispatch之前预处理函数
	//		// 当需要中断处理，即不需要后续的dispatcher处理则需要返回错误
	//		if functor != nil {
	//			if ret, err = functor(item); err != nil {
	//				continue
	//			}
	//		} else {
	//			// 如果没有设置用户自己业务，则仅仅做转发使用
	//			ret = item
	//		}
	//
	//		for sub, _ := range d.downDispatchers {
	//			sub.Dispatch(ret)
	//		}
	//
	//		// dispatch之后处理函数
	//		if d.sufFunctor != nil {
	//			//d.sufFunctor(item)
	//		}
	//	}
	//}

	//d.logf(LogLevelInfo, " %dth process quiting...\n", id)
	//atomic.AddUint32(&d.chanCount, ^uint32(0))
}

func (d *ProcessorBuilder) groupExecute(functor Functor) {
	defer d.wg.Done()
	name := runtime.FuncForPC(reflect.ValueOf(functor).Pointer()).Name()
	defer utils.TimeCost(time.Now(), d.processData.LogHead+"groupExecute", name)
	// 经过用户业务处理之后的返回值，可以简简单单返回item自身
	var ret interface{}
	var err error
	funcName := runtime.FuncForPC(reflect.ValueOf(functor).Pointer()).Name()
	if d.err != nil {
		logging.Errorf(d.processData.LogHead+"groupExecute err,ProcessorBuilder:%+v,funcName:%v, err:%+v", d, funcName, err)
		return
	}

	// dispatch之前预处理函数
	// 当需要中断处理，即不需要后续的dispatcher处理则需要返回错误
	if functor != nil {
		if ret, err = functor(d.processData); err != nil {
			logging.Errorf(d.processData.LogHead+"groupExecute err,ProcessorBuilder:%+v,funcName:%v, err:%+v", d, funcName, err)
			d.err = err
			return
		}
	}
	logging.Infof(d.processData.LogHead+"groupExecute end ret:%v,err:%v", ret, err)
}

func (d *ProcessorBuilder) execute(functor Functor) {
	if functor == nil {
		return
	}
	// 经过用户业务处理之后的返回值，可以简简单单返回item自身
	var ret interface{}
	var err error
	funcName := runtime.FuncForPC(reflect.ValueOf(functor).Pointer()).Name()
	if d.err != nil {
		logging.Errorf(d.processData.LogHead+"execute err,ProcessorBuilder:%+v,funcName:%v, err:%+v", d, funcName, err)
		return
	}
	defer utils.TimeCost(time.Now(), d.processData.LogHead+"execute", funcName)
	// dispatch之前预处理函数
	// 当需要中断处理，即不需要后续的dispatcher处理则需要返回错误
	if ret, err = functor(d.processData); err != nil {
		logging.Errorf(d.processData.LogHead+"execute err,ProcessorBuilder:%+v,funcName:%v, err:%+v", d, funcName, err)
		d.err = err
		return
	}
	logging.Infof(d.processData.LogHead+"execute end ret:%v,err:%v", ret, err)
}

func (d *ProcessorBuilder) setExecuteMap(preFunctor Functor, concurrencyMark int, concurrencyMap *orderedmap.OrderedMap) {
	d.mutex.Lock()
	if concurrencyMap == nil {
		concurrencyMap = orderedmap.NewOrderedMap()
	}
	functorArray, _ := concurrencyMap.Get(concurrencyMark)
	if functorArray == nil {
		functorArray = []Functor{}
	}
	functorArray = append(functorArray.([]Functor), preFunctor)
	concurrencyMap.Set(concurrencyMark, functorArray)
	d.mutex.Unlock()
}

func (d *ProcessorBuilder) Dispatch(item interface{}) {
	d.msgChan <- item
}

func (d *ProcessorBuilder) Ack(result interface{}) error {
	// TODO:是否合理???
	// DefaultDispatcher不应该调用上游Dispatcher，应由继承者来实现
	// for up, _ := range d.upDispatchers {
	// up.Ack()
	// }
	return nil
}

// 一些定时任务，比如打印状态，更新缓存。。。
// 使用时建议使用一个单独协程，
// 由首个dispatcher调用，触犯后面dispatcher链
// framework := new(ProcessorBuilder)
// go func(){
// tick := time.NewTicker(time.Duration(20) * time.Second)
// for {
// <- tick.C
// framework.Tick()
// }
// }()
// defer tick.Stop()
func (d *ProcessorBuilder) Tick() {
	for sub, _ := range d.downDispatchers {
		sub.Tick()
	}
}

func (d *ProcessorBuilder) Close() {
	close(d.msgChan)
	d.wg.Wait()

	d.running = false

	// 关闭下游dispatcher
	for sub, _ := range d.downDispatchers {
		if sub.IsRunning() {
			sub.Close()
		}

		for {
			if sub.IsRunning() {
				time.Sleep(time.Second)
			} else {
				break
			}
		}
	}
}
