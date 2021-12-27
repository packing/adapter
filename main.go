package main

import (
    "flag"
    "fmt"
    "log"
    "os"
    "runtime"
    "runtime/pprof"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/packing/clove/codecs"
    "github.com/packing/clove/env"
    "github.com/packing/clove/messages"
    "github.com/packing/clove/nnet"
    "github.com/packing/clove/packets"
    "github.com/packing/clove/utils"
)

var (
    help    bool
    version bool

    daemon   bool

    pprofFile string
    addr      string
    slaveAddr string
    unixAddr  string
    unixMsgAddr string

    onlyTCP   bool

    localhost string

    logDir   string
    logLevel = utils.LogLevelInfo
    pidFile  string

    tcp      *nnet.TCPServer = nil
    tcpCtrl  *nnet.TCPClient = nil
    unix     *nnet.UnixUDP   = nil
    unixMsg  *nnet.UnixMsg   = nil
)

func usage() {
    fmt.Fprint(os.Stderr, `adapter

Usage: adapter [-hv] [-d daemon] [-f pprof file] [-c master addr] [-l loglevel]

Options:
`)
    flag.PrintDefaults()
}

var flowadd uint64 = 0

func notify(tp int, c nnet.Controller) {
    msg := messages.CreateS2SMessage(messages.ProtocolTypeDeliver)
    msg.SetTag(messages.ProtocolTagSlave)

    realMsg := messages.CreateS2SMessage(tp)
    realMsg.SetSessionId([]nnet.SessionID{c.GetSessionID()})
    realMsg.SetTag(messages.ProtocolTagSlave)

    body := make(codecs.IMMap)
    body[messages.ProtocolKeyHost] = c.GetSource()
    realMsg.SetBody(body)

    realMsgData, _ := messages.DataFromMessage(realMsg)
    realMsgMap, ok := realMsgData.(codecs.IMMap)
    if !ok {
        return
    }

    msg.SetBody(realMsgMap)

    ssid, si := pollFreeSlave()
    if si != nil {
        if si.host == localhost && si.unixAddr != "" {
            data, err := messages.DataFromMessage(msg)
            if err == nil {
                mapSend, ok := data.(codecs.IMMap)
                if ok {
                    mapSend[messages.ProtocolKeyUnixAddr] = unixAddr
                }
                unix.SendTo(si.unixAddr, mapSend)
            }
        } else {
            msg.SetSessionId([]nnet.SessionID{ssid})
            data, err := messages.DataFromMessage(msg)
            if err == nil {
                tcpCtrl.Send(data)
            }
        }
    }
}

var c2sin uint64 = 0
var c2sout uint64 = 0

func OnC2SDataDecoded(controller nnet.Controller, addr string, data codecs.IMData) error {
    atomic.AddUint64(&c2sin, 1)
    defer atomic.AddUint64(&c2sout, 1)

    tcpClient, ok := controller.(*nnet.TCPController)
    if !ok {
        return fmt.Errorf("ontroller is not a TCPController")
    }
    mapData, ok := data.(codecs.IMMap)
    if !ok {
        tcpClient.UnlockProcess()
        return messages.ErrorDataNotIsMessageMap
    }

    sess := make(codecs.IMSlice, 1)
    sess[0] = controller.GetSessionID()
    mapData[messages.ProtocolKeySessionId] = sess

    reader := codecs.CreateMapReader(mapData)
    itags := reader.TryReadValue(messages.ProtocolKeyTag)

    isAdapterMsg := false
    if itags != nil {
        tags, ok := itags.(codecs.IMSlice)
        if ok {
            for _, tag := range tags {
                if tag == messages.ProtocolTagAdapter {
                    isAdapterMsg = true
                    break
                }
            }
        }
    }

    if isAdapterMsg {
        err := messages.GlobalMessageQueue.Push(controller, addr, mapData)
        tcpClient.UnlockProcess()
        return err
    } else {
        atomic.AddUint64(&flowadd, 1)
        msg := messages.CreateS2SMessage(messages.ProtocolTypeDeliver)
        msg.SetTag(messages.ProtocolTagSlave)
        if tcpClient.IsFlowMode() {
            msg.SetErrorCode(-1)
        }
        msg.SetBody(mapData)
        ssid, si := pollFreeSlave()
        if si != nil {
            if si.host == localhost && si.unixAddr != "" {
                data, err := messages.DataFromMessage(msg)
                if err == nil {
                    mapSend, ok := data.(codecs.IMMap)
                    if ok {
                        mapSend[messages.ProtocolKeyUnixAddr] = unixAddr
                    }
                    unix.SendTo(si.unixAddr, mapSend)
                }
            } else {
                msg.SetSessionId([]nnet.SessionID{ssid})
                data, err := messages.DataFromMessage(msg)
                if err == nil {
                    tcpCtrl.Send(data)
                }
            }
        }
    }

    return nil
}

func OnS2SDataDecoded(controller nnet.Controller, addr string, data codecs.IMData) error {
    mapData, ok := data.(codecs.IMMap)
    if !ok {
        return messages.ErrorDataNotIsMessageMap
    }

    reader := codecs.CreateMapReader(mapData)
    itags := reader.TryReadValue(messages.ProtocolKeyTag)

    isAdapterMsg := false
    tags, ok := itags.(codecs.IMSlice)
    if ok {
        for _, tag := range tags {
            if tag == messages.ProtocolTagAdapter {
                isAdapterMsg = true
                break
            }
        }
    }

    if isAdapterMsg {
        return messages.GlobalMessageQueue.Push(controller, addr, mapData)
    } else {
        tcpClient, ok := controller.(*nnet.TCPController)
        if ok {
            tcpClient.UnlockProcess()
        }
    }

    return nil
}

func sayHello() error {
    defer func() {
        utils.LogPanic(recover())
    }()
    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapterHello)
    msg.SetTag(messages.ProtocolTagMaster)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyId] = os.Getpid()
    req[messages.ProtocolKeyValue] = tcp.GetTotal()
    if !onlyTCP {
        req[messages.ProtocolKeyUnixAddr] = unixAddr
    }
    req[messages.ProtocolKeyUnixMsgAddr] = unixMsgAddr

    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        tcpCtrl.Send(pck)
    }
    return err
}

func reportState() error {
    defer func() {
        utils.LogPanic(recover())
    }()
    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapterChange)
    msg.SetTag(messages.ProtocolTagMaster)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyValue] = tcp.GetTotal()
    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        tcpCtrl.Send(pck)
    }
    return err
}

func main() {

    runtime.GOMAXPROCS(2)

    flag.BoolVar(&help, "h", false, "this help")
    flag.BoolVar(&version, "v", false, "print version")
    flag.BoolVar(&daemon, "d", false, "run at daemon")
    flag.BoolVar(&onlyTCP, "t", false, "only tcp tunnel")
    flag.StringVar(&addr, "c", "127.0.0.1:10088", "controller addr")
    flag.StringVar(&pprofFile, "f", "", "pprof file")
    flag.IntVar(&logLevel, "l", logLevel, "log level.(verbose:0,info:1,warn:2,error:3).default: 1")
    flag.Usage = usage

    flag.Parse()
    if help {
        flag.Usage()
        return
    }
    if version {
        fmt.Println("adapter version 1.0")
        return
    }

    logDir = "./logs/adapter"
    if !daemon {
        logDir = ""
    } else {
        if os.Getppid() != 1 {
            utils.Daemon()
            return
        }
    }

    pidFile = "./pid"
    utils.GeneratePID(pidFile)

    unixAddr = fmt.Sprintf("/tmp/adapter_%d.sock", os.Getpid())
    unixMsgAddr = fmt.Sprintf("/tmp/adapter_msg_%d.sock", os.Getpid())

    var pproff *os.File = nil
    if pprofFile != "" {
        pf, err := os.OpenFile(pprofFile, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            log.Fatal(err)
        }
        pproff = pf
        pprof.StartCPUProfile(pproff)
    }

    defer func() {
        if pproff != nil {
            pprof.StopCPUProfile()
            pproff.Close()
        }

        utils.RemovePID(pidFile)

        syscall.Unlink(unixMsgAddr)
        syscall.Unlink(unixAddr)

        utils.LogInfo(">>> 进程已退出")
    }()

    utils.LogInit(logLevel, logDir)
    //注册解码器
    //codecs.CodecIMv2.Decoder.SetByteOrder(binary.LittleEndian)
    //codecs.CodecIMv2.Encoder.SetByteOrder(binary.LittleEndian)

    env.RegisterCodec(codecs.CodecIMv2)
    env.RegisterCodec(codecs.CodecJSONv1)

    //注册通信协议
    env.RegisterPacketFormat(packets.PacketFormatNB)
    env.RegisterPacketFormat(packets.PacketFormatHTTP)
    env.RegisterPacketFormat(packets.PacketFormatWS)

    nnet.SetSendBufSize(10240)
    nnet.SetRecvBufSize(10240)

    nnet.SetWebsocketDefaultCodec(codecs.CodecIMv2)

    //创建s2s管道
    _, err := os.Stat(unixAddr)
    if err == nil || !os.IsNotExist(err) {
        err = os.Remove(unixAddr)
        if err != nil {
            utils.LogError("无法删除unix管道旧文件", err)
        }
    }
    _, err = os.Stat(unixMsgAddr)
    if err == nil || !os.IsNotExist(err) {
        err = os.Remove(unixMsgAddr)
        if err != nil {
            utils.LogError("无法删除unix句柄管道旧文件", err)
        }
    }
    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagAdapter, AdapterMessageObject{})
    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeC2S, messages.ProtocolTagAdapter, ClientMessageObject{})
    messages.GlobalDispatcher.Dispatch()



    unix = nnet.CreateUnixUDPWithFormat(packets.PacketFormatNB, codecs.CodecIMv2)
    unix.OnDataDecoded = messages.GlobalMessageQueue.Push
    err = unix.Bind(unixAddr)
    if err != nil {
        utils.LogError("!!!无法创建unix管道 %s", unixAddr, err)
        unix.Close()
        return
    }

    tcp = nnet.CreateTCPServer()
    tcp.OnDataDecoded = OnC2SDataDecoded
    tcp.ControllerCome = func(controller nnet.Controller) error {
        //tcpClient, ok := controller.(*nnet.TCPController)
        //if ok {
            //tcpClient.SetFlowMode(true)
            //tcpClient.SetFlowQueueLimit(64)
        //}
        return nil
    }
    tcp.OnWelcome = func(controller nnet.Controller) error {
        notify(messages.ProtocolTypeClientEnter, controller)
        return nil
    }
    tcp.OnBye = func(controller nnet.Controller) error {
        notify(messages.ProtocolTypeClientLeave, controller)
        utils.LogInfo("OnBye => %s", controller.GetSource())
        return nil
    }
    tcp.ServeWithoutListener()

    unixMsg = nnet.CreateUnixMsg()
    unixMsg.SetControllerAssociatedObject(tcp)
    err = unixMsg.Bind(unixMsgAddr)
    if err != nil {
        utils.LogError("!!!无法创建unix句柄管道 %s", unixMsgAddr, err)
        unix.Close()
        unixMsg.Close()
        return
    }

    tcpCtrl = nnet.CreateTCPClient(packets.PacketFormatNB, codecs.CodecIMv2)
    tcpCtrl.OnDataDecoded = messages.GlobalMessageQueue.Push
    err = tcpCtrl.Connect(addr, 0)
    if err != nil {
        utils.LogError("!!!无法连接到控制服务器 %s", addr, err)
        unix.Close()
        unixMsg.Close()
        tcpCtrl.Close()
        return
    } else {
        sayHello()
    }

    /*
    go func() {
        for {
            if !daemon {
                //agvt, tmax, tmin := messages.GlobalDispatcher.GetAsyncInfo()
                fmt.Printf(">>> 当前 网络 = [TCP读: %d, TCP写: %d, UNIX读: %d, UNIX写: %d, 句柄: %d, 连接: %d, 加解锁: %d / %d / %d, deliver: %d / %d, c2s: %d / %d]\r",
                    //float64(agvt)/float64(time.Millisecond), float64(tmin)/float64(time.Millisecond), float64(tmax)/float64(time.Millisecond),
                    //float64(nnet.GetEncodeAgvTime())/float64(time.Millisecond), float64(nnet.GetDecodeAgvTime())/float64(time.Millisecond),
                    nnet.GetTotalTcpRecvSize(),
                    nnet.GetTotalTcpSendSize(),
                    nnet.GetTotalUnixRecvSize(),
                    nnet.GetTotalUnixSendSize(),
                    nnet.GetTotalHandleSendSize(),
                    tcp.GetTotal(),
                        flowadd, flowret, flowrets, deliverin, deliverout, c2sin, c2sout)
            }
            runtime.Gosched()
            time.Sleep(1 * time.Second)
        }
    }()*/

    go func() {
        for {
            time.Sleep(10 * time.Second)
            reportState()
            runtime.Gosched()
        }
    }()

    utils.LogInfo(">>> 当前协程数量 > %d", runtime.NumGoroutine())
    //开启调度，主线程停留在此等候信号
    env.Schedule()

    tcpCtrl.Close()
    unixMsg.Close()
    unix.Close()
    tcp.Close()

}
