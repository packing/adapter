package main

import (
    "sync"

    "github.com/packing/clove/nnet"
)

type SlaveInfo struct {
    pid int
    host string
    vmFree int
    unixAddr string
}

var (
    GlobalSlaves = make(map[nnet.SessionID] SlaveInfo)

    slaveLock sync.Mutex
)

func addSlave(slaveId nnet.SessionID, si SlaveInfo) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    GlobalSlaves[slaveId] = si
}

func pollFreeSlave() (nnet.SessionID, *SlaveInfo) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    var poll *SlaveInfo = nil
    var ssid nnet.SessionID = 0
    for sid, si := range GlobalSlaves {
        if poll == nil {
            poll = &si
            ssid = sid
            continue
        }
        if si.vmFree > 0 && si.vmFree > poll.vmFree {
            poll = &si
            ssid = sid
        }
    }
    return ssid, poll
}

func delSlave(slaveId nnet.SessionID) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    delete(GlobalSlaves, slaveId)
}
