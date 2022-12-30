package processor

// 配置文件从ini改为json，json能够更好的处理复杂的层级关系
// http://www.guyannanfei25.site/2017/05/14/misc-talk/
type Processor interface {
	Init(data interface{}) error
	SetName(name string)
	GetName() string

	// 只有需要级联的dispatch才需要Register接口
	// 注册下游dispatcher
	DownRegister(d Processor)

	// 注册上游dispatcher
	UpRegister(d Processor)

	// Dispatch前预处理函数
	SetPreFunc(preFunctor Functor, concurrencyMark int)

	// Dispatch后处理函数
	SetSufFuc(sufFunctor Functor, concurrencyMark int)

	// 开始运行，在注册好dispatcher并设置好预处理函数之后
	Start()

	// 是否正在运行
	IsRunning() bool

	Dispatch(item interface{})
	Ack(result interface{}) error
	// 一些定时任务，比如打印状态，更新缓存。。。
	Tick()
	Close()
}
