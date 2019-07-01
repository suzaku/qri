// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package log_fbs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Suggestion struct {
	_tab flatbuffers.Table
}

func GetRootAsSuggestion(buf []byte, offset flatbuffers.UOffsetT) *Suggestion {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Suggestion{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Suggestion) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Suggestion) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Suggestion) Timestamp() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Suggestion) Text() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Suggestion) Subject() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Suggestion) Type() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func SuggestionStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func SuggestionAddTimestamp(builder *flatbuffers.Builder, timestamp flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(timestamp), 0)
}
func SuggestionAddText(builder *flatbuffers.Builder, text flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(text), 0)
}
func SuggestionAddSubject(builder *flatbuffers.Builder, subject flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(subject), 0)
}
func SuggestionAddType(builder *flatbuffers.Builder, type_ flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(type_), 0)
}
func SuggestionEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
