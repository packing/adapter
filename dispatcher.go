package main

import (
    "fmt"
    "sync/atomic"

    "github.com/packing/clove/codecs"
    "github.com/packing/clove/messages"
    "github.com/packing/clove/nnet"
    "github.com/packing/clove/utils"
)

type AdapterMessageObject struct {
}

var flowret uint64 = 0
var flowrets uint64 = 0
var deliverin uint64 = 0
var deliverout uint64 = 0

func OnFlowReturn(msg *messages.Message) error {
    atomic.AddUint64(&flowret, 1)
    sessionIds := msg.GetSessionId()
    if sessionIds == nil || len(sessionIds) == 0 {
        return fmt.Errorf("missing key attribute sessionid for The OnFlowReturn")
    }
    client := tcp.GetController(sessionIds[0])
    if client != nil {
        client.UnlockProcess()
        atomic.AddUint64(&flowrets, 1)
    }
    return nil
}

func OnDeliver(msg *messages.Message) error {
    atomic.AddUint64(&deliverin, 1)
    defer atomic.AddUint64(&deliverout, 1)
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    iSsids, ok := body[messages.ProtocolKeySessionId]
    if ok {
        sess, _ := iSsids.(codecs.IMSlice)
        ssids := make([]nnet.SessionID, len(sess))
        for i, sid := range sess {
            ssid := codecs.Uint64FromInterface(sid)
            ssids[i] = nnet.SessionID(ssid)
        }
        delete(body, messages.ProtocolKeySessionId)
        tcp.Mutilcast(ssids, body)
    } else {
        tcp.Boardcast(body)
    }

    return nil
}

func OnKill(msg *messages.Message) error {

    body := msg.GetBody()
    if body == nil {
        return nil
    }

    reader := codecs.CreateMapReader(body)
    iSessionIds := reader.TryReadValue(messages.ProtocolKeySessionId)

    if iSessionIds == nil {
        return nil
    }

    sessionIds, ok := iSessionIds.(codecs.IMSlice)
    if ok {
        for _, session := range sessionIds {
            sid := codecs.Int64FromInterface(session)
            if sid > 0 {
                e := tcp.CloseController(nnet.SessionID(sid))
                if e == nil {
                    utils.LogInfo("踢出客户端 %d 的连接", sid)
                }
            }
        }
    }

    return nil
}

func OnSlaves(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    iLocalhost, ok := body[messages.ProtocolKeyLocalHost]
    if ok {
        localhost, _ = iLocalhost.(string)
    }
    iList, ok := body[messages.ProtocolKeyValue]
    if ok {
        list, ok := iList.(codecs.IMSlice)
        if ok {
            for _, l := range list {
                m, ok := l.(codecs.IMMap)
                if ok {
                    mr := codecs.CreateMapReader(m)
                    sessionId := mr.UintValueOf(messages.ProtocolKeySessionId, 0)
                    if sessionId > 0 {
                        var si SlaveInfo
                        si.vmFree = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
                        si.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
                        si.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
                        si.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
                        addSlave(nnet.SessionID(sessionId), si)
                    }
                }
            }
        }
    }
    utils.LogInfo("Slave 列表接收完毕, 本机主机为: %s", localhost)

    return nil
}

func OnSlaveCome(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    sessionId := mr.UintValueOf(messages.ProtocolKeySessionId, 0)
    if sessionId > 0 {
        var si SlaveInfo
        si.vmFree = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
        si.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
        si.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
        si.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
        addSlave(nnet.SessionID(sessionId), si)
        utils.LogInfo("Slave %s - %d (%d) 上线", si.host, si.pid, sessionId)
    }

    return nil
}

func OnSlaveBye(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    sessionId := mr.UintValueOf(messages.ProtocolKeySessionId, 0)
    if sessionId > 0 {
        delSlave(nnet.SessionID(sessionId))
        utils.LogInfo("Slave (%d) 离线", sessionId)
    }

    return nil
}

func OnSlaveChange(msg *messages.Message) error {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    sessionId := mr.UintValueOf(messages.ProtocolKeySessionId, 0)
    if sessionId > 0 {
        var si SlaveInfo
        si.vmFree = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
        si.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
        si.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
        si.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
        addSlave(nnet.SessionID(sessionId), si)
        //utils.LogInfo("Slave %s - %d (%d) 状态更新", si.host, si.pid, sessionId)
    }

    return nil
}

func (receiver AdapterMessageObject) GetMappedTypes() map[int]messages.MessageProcFunc {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeFlowReturn] = OnFlowReturn
    msgMap[messages.ProtocolTypeDeliver] = OnDeliver
    msgMap[messages.ProtocolTypeKillClient] = OnKill
    msgMap[messages.ProtocolTypeSlaves] = OnSlaves
    msgMap[messages.ProtocolTypeSlaveCome] = OnSlaveCome
    msgMap[messages.ProtocolTypeSlaveBye] = OnSlaveBye
    msgMap[messages.ProtocolTypeSlaveChange] = OnSlaveChange
    return msgMap
}

type ClientMessageObject struct {
}

func OnHeart(msg *messages.Message) error {
    defer func() {
        tcpClient, ok := msg.GetController().(*nnet.TCPController)
        if ok {
            tcpClient.UnlockProcess()
        }
    }()
    body := msg.GetBody()
    if body == nil {
        return nil
    }
    data, err := messages.DataFromMessage(msg)
    if err == nil {
        _, err = tcp.Send(msg.GetSessionId()[0], data)
    }
    return nil
}

func (receiver ClientMessageObject) GetMappedTypes() map[int]messages.MessageProcFunc {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeHeart] = OnHeart
    return msgMap
}
