// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package log_fbs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Log struct {
	_tab flatbuffers.Table
}

func GetRootAsLog(buf []byte, offset flatbuffers.UOffsetT) *Log {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Log{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Log) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Log) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Log) Subject() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Log) Note() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Log) Type() LogType {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt8(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Log) MutateType(n LogType) bool {
	return rcv._tab.MutateInt8Slot(8, n)
}

func LogStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func LogAddSubject(builder *flatbuffers.Builder, subject flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(subject), 0)
}
func LogAddNote(builder *flatbuffers.Builder, note flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(note), 0)
}
func LogAddType(builder *flatbuffers.Builder, type_ int8) {
	builder.PrependInt8Slot(2, type_, 0)
}
func LogEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
