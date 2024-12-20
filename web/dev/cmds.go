package dev

import "github.com/banbox/banbot/config"

type CmdArgs struct {
	Port     int
	Host     string
	Configs  config.ArrString
	DataDir  string
	LogLevel string
	LogFile  string
	TimeZone string
	DBFile   string
}
