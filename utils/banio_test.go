package utils

import (
	"github.com/banbox/banexg/log"
	"github.com/bytedance/sonic"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestBanServer(t *testing.T) {
	server := NewBanServer("127.0.0.1:6789", "spider")
	err := server.RunForever()
	if err != nil {
		panic(err)
	}
}

func TestBanClient(t *testing.T) {
	client, err := NewClientIO("127.0.0.1:6789")
	if err != nil {
		panic(err)
	}
	go client.RunForever()
	err = client.SetVal(&KeyValExpire{
		Key: "vv1",
		Val: "vvvv",
	})
	if err != nil {
		panic(err)
	}
	val, err := client.GetVal("vv1", 5)
	if err != nil {
		panic(err)
	}
	text, _ := sonic.MarshalString(val)
	log.Info("get val of vv1", zap.String("val", text))
	lockVal, err := GetNetLock("lk1", 5)
	if err != nil {
		panic(err)
	}
	log.Info("set lock", zap.Int32("val", lockVal))
	val, err = client.GetVal("lock_lk1", 5)
	if err != nil {
		panic(err)
	}
	text, _ = sonic.MarshalString(val)
	log.Info("lock real val", zap.String("val", text))
	time.Sleep(time.Second * 3)
	err = DelNetLock("lk1", lockVal)
	if err != nil {
		panic(err)
	}
	val, err = client.GetVal("lock_lk1", 5)
	if err != nil {
		panic(err)
	}
	text, _ = sonic.MarshalString(val)
	log.Info("lock val after del", zap.String("val", text))
}
