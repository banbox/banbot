# 数据库概述
需要存储到数据库的有三类：K线数据、交易数据、UI相关数据。  
K线与相关元数据使用 QuestDB（PGWire）存储，并用 `sranges` 记录已下载/无数据区间，允许数据不连续。  
为确保灵活性，交易数据(ormo)和UI相关数据(ormu)使用独立的sqlite文件存储。  
ormo/ormu依赖orm，不可反向依赖，避免出现依赖环


# 从proto生成go代码
安装protoc:
```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```
生成：
```shell
protoc --go_out=. --go_opt=paths=source_relative kdata.proto
```
注意将生成后的`kdata.pb.go`中`package __`改为`package orm`
