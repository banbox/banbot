// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.27.2
// source: aifea.proto

package biz

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SubReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Exchange string `protobuf:"bytes,1,opt,name=exchange,proto3" json:"exchange,omitempty"`
	Market   string `protobuf:"bytes,2,opt,name=market,proto3" json:"market,omitempty"`
	Pair     string `protobuf:"bytes,3,opt,name=pair,proto3" json:"pair,omitempty"`
	Start    int64  `protobuf:"varint,4,opt,name=start,proto3" json:"start,omitempty"`
	End      int64  `protobuf:"varint,5,opt,name=end,proto3" json:"end,omitempty"`
	Task     string `protobuf:"bytes,6,opt,name=task,proto3" json:"task,omitempty"`
	Sample   int32  `protobuf:"varint,7,opt,name=sample,proto3" json:"sample,omitempty"`
}

func (x *SubReq) Reset() {
	*x = SubReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aifea_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubReq) ProtoMessage() {}

func (x *SubReq) ProtoReflect() protoreflect.Message {
	mi := &file_aifea_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubReq.ProtoReflect.Descriptor instead.
func (*SubReq) Descriptor() ([]byte, []int) {
	return file_aifea_proto_rawDescGZIP(), []int{0}
}

func (x *SubReq) GetExchange() string {
	if x != nil {
		return x.Exchange
	}
	return ""
}

func (x *SubReq) GetMarket() string {
	if x != nil {
		return x.Market
	}
	return ""
}

func (x *SubReq) GetPair() string {
	if x != nil {
		return x.Pair
	}
	return ""
}

func (x *SubReq) GetStart() int64 {
	if x != nil {
		return x.Start
	}
	return 0
}

func (x *SubReq) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

func (x *SubReq) GetTask() string {
	if x != nil {
		return x.Task
	}
	return ""
}

func (x *SubReq) GetSample() int32 {
	if x != nil {
		return x.Sample
	}
	return 0
}

type ArrMap struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Mats map[string]*NumArr `protobuf:"bytes,1,rep,name=Mats,proto3" json:"Mats,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *ArrMap) Reset() {
	*x = ArrMap{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aifea_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ArrMap) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ArrMap) ProtoMessage() {}

func (x *ArrMap) ProtoReflect() protoreflect.Message {
	mi := &file_aifea_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ArrMap.ProtoReflect.Descriptor instead.
func (*ArrMap) Descriptor() ([]byte, []int) {
	return file_aifea_proto_rawDescGZIP(), []int{1}
}

func (x *ArrMap) GetMats() map[string]*NumArr {
	if x != nil {
		return x.Mats
	}
	return nil
}

type NumArr struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data  []float64 `protobuf:"fixed64,1,rep,packed,name=data,proto3" json:"data,omitempty"`
	Shape []int32   `protobuf:"varint,2,rep,packed,name=shape,proto3" json:"shape,omitempty"`
}

func (x *NumArr) Reset() {
	*x = NumArr{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aifea_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NumArr) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NumArr) ProtoMessage() {}

func (x *NumArr) ProtoReflect() protoreflect.Message {
	mi := &file_aifea_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NumArr.ProtoReflect.Descriptor instead.
func (*NumArr) Descriptor() ([]byte, []int) {
	return file_aifea_proto_rawDescGZIP(), []int{2}
}

func (x *NumArr) GetData() []float64 {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *NumArr) GetShape() []int32 {
	if x != nil {
		return x.Shape
	}
	return nil
}

var File_aifea_proto protoreflect.FileDescriptor

var file_aifea_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x61, 0x69, 0x66, 0x65, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa4, 0x01,
	0x0a, 0x06, 0x53, 0x75, 0x62, 0x52, 0x65, 0x71, 0x12, 0x1a, 0x0a, 0x08, 0x65, 0x78, 0x63, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x78, 0x63, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x74, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x74, 0x12, 0x12, 0x0a, 0x04,
	0x70, 0x61, 0x69, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x70, 0x61, 0x69, 0x72,
	0x12, 0x14, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x61, 0x73, 0x6b,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x61, 0x73, 0x6b, 0x12, 0x16, 0x0a, 0x06,
	0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x73, 0x61,
	0x6d, 0x70, 0x6c, 0x65, 0x22, 0x71, 0x0a, 0x06, 0x41, 0x72, 0x72, 0x4d, 0x61, 0x70, 0x12, 0x25,
	0x0a, 0x04, 0x4d, 0x61, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x41,
	0x72, 0x72, 0x4d, 0x61, 0x70, 0x2e, 0x4d, 0x61, 0x74, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52,
	0x04, 0x4d, 0x61, 0x74, 0x73, 0x1a, 0x40, 0x0a, 0x09, 0x4d, 0x61, 0x74, 0x73, 0x45, 0x6e, 0x74,
	0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x6b, 0x65, 0x79, 0x12, 0x1d, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4e, 0x75, 0x6d, 0x41, 0x72, 0x72, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x32, 0x0a, 0x06, 0x4e, 0x75, 0x6d, 0x41, 0x72,
	0x72, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x01, 0x52,
	0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x68, 0x61, 0x70, 0x65, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x05, 0x52, 0x05, 0x73, 0x68, 0x61, 0x70, 0x65, 0x32, 0x30, 0x0a, 0x09, 0x46,
	0x65, 0x61, 0x46, 0x65, 0x65, 0x64, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x0b, 0x53, 0x75, 0x62, 0x46,
	0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x12, 0x07, 0x2e, 0x53, 0x75, 0x62, 0x52, 0x65, 0x71,
	0x1a, 0x07, 0x2e, 0x41, 0x72, 0x72, 0x4d, 0x61, 0x70, 0x22, 0x00, 0x30, 0x01, 0x32, 0x42, 0x0a,
	0x06, 0x41, 0x49, 0x6e, 0x66, 0x65, 0x72, 0x12, 0x1b, 0x0a, 0x05, 0x54, 0x72, 0x65, 0x6e, 0x64,
	0x12, 0x07, 0x2e, 0x41, 0x72, 0x72, 0x4d, 0x61, 0x70, 0x1a, 0x07, 0x2e, 0x41, 0x72, 0x72, 0x4d,
	0x61, 0x70, 0x22, 0x00, 0x12, 0x1b, 0x0a, 0x05, 0x54, 0x72, 0x61, 0x64, 0x65, 0x12, 0x07, 0x2e,
	0x41, 0x72, 0x72, 0x4d, 0x61, 0x70, 0x1a, 0x07, 0x2e, 0x41, 0x72, 0x72, 0x4d, 0x61, 0x70, 0x22,
	0x00, 0x42, 0x08, 0x5a, 0x06, 0x2e, 0x2e, 0x2f, 0x62, 0x69, 0x7a, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_aifea_proto_rawDescOnce sync.Once
	file_aifea_proto_rawDescData = file_aifea_proto_rawDesc
)

func file_aifea_proto_rawDescGZIP() []byte {
	file_aifea_proto_rawDescOnce.Do(func() {
		file_aifea_proto_rawDescData = protoimpl.X.CompressGZIP(file_aifea_proto_rawDescData)
	})
	return file_aifea_proto_rawDescData
}

var file_aifea_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_aifea_proto_goTypes = []interface{}{
	(*SubReq)(nil), // 0: SubReq
	(*ArrMap)(nil), // 1: ArrMap
	(*NumArr)(nil), // 2: NumArr
	nil,            // 3: ArrMap.MatsEntry
}
var file_aifea_proto_depIdxs = []int32{
	3, // 0: ArrMap.Mats:type_name -> ArrMap.MatsEntry
	2, // 1: ArrMap.MatsEntry.value:type_name -> NumArr
	0, // 2: FeaFeeder.SubFeatures:input_type -> SubReq
	1, // 3: AInfer.Trend:input_type -> ArrMap
	1, // 4: AInfer.Trade:input_type -> ArrMap
	1, // 5: FeaFeeder.SubFeatures:output_type -> ArrMap
	1, // 6: AInfer.Trend:output_type -> ArrMap
	1, // 7: AInfer.Trade:output_type -> ArrMap
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_aifea_proto_init() }
func file_aifea_proto_init() {
	if File_aifea_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_aifea_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SubReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_aifea_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ArrMap); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_aifea_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NumArr); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_aifea_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_aifea_proto_goTypes,
		DependencyIndexes: file_aifea_proto_depIdxs,
		MessageInfos:      file_aifea_proto_msgTypes,
	}.Build()
	File_aifea_proto = out.File
	file_aifea_proto_rawDesc = nil
	file_aifea_proto_goTypes = nil
	file_aifea_proto_depIdxs = nil
}