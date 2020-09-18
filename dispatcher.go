package main

import (
    "nbpyraw/nnet"
    "reflect"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/utils"
)

type AdapterMessageObject struct {
}

func OnDeliver(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }


    iSsids, ok := body[messages.ProtocolKeySessionId]
    if ok {
        sess, _ := iSsids.(codecs.IMSlice)
        ssids := make([]nnet.SessionID, len(sess))
        for i, sid := range sess {
            ssid := reflect.ValueOf(sid).Uint()
            ssids[i] = nnet.SessionID(ssid)
        }
        delete(body, messages.ProtocolKeySessionId)
        tcp.Mutilcast(ssids, body)
    } else {
        tcp.Boardcast(body)
    }

    return nil
}

func OnKill(msg *messages.Message) (error) {
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
            sid, ok := session.(int64)
            if ok {
                utils.LogInfo("踢出客户端 %d 的连接", sid)
            }
        }
    }

    return nil
}

func OnSlaves(msg *messages.Message) (error) {
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
                    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
                    sessionId, ok := iSessionId.(uint)
                    if ok {
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

    return nil
}

func OnSlaveCome(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
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

func OnSlaveBye(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
        delSlave(nnet.SessionID(sessionId))
        utils.LogInfo("Slave (%d) 离线", sessionId)
    }

    return nil
}

func OnSlaveChange(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
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

func (receiver AdapterMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
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

func OnHeart(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }
    //reader := codecs.CreateMapReader(body)
    //tv := reader.IntValueOf(messages.ProtocolKeyValue, 0)
    //utils.LogInfo("客户端心跳, time -> %d", tv, msg.GetSessionId())

    //body[messages.ProtocolKeyValue] = time.Now().UnixNano()
    //msg.SetBody(body)
    //msg.SetScheme(messages.ProtocolSchemeS2C)
    //msg.SetTag(messages.ProtocolTagClient)
    data, err := messages.DataFromMessage(msg)
    if err == nil {
        _, err = tcp.Send(msg.GetSessionId()[0], data)
    }
    //tcp.Send(msg.GetSessionId()[0], msg.GetSrcData())
    return nil
}

func (receiver ClientMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeHeart] = OnHeart
    return msgMap
}