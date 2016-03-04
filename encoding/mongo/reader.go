package mongo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

func fieldSize(fld interface{}) int {
	switch fld.(type) {
	case int32, *int32:
		return 4
	case int64, *int64:
		return 8
	}
	return 0
}

type MessageReader struct {
	byteReader *bytes.Buffer // Underlying byte reader.
	b          []byte        // Slice holding the message data.
	i          int64         // Current reading index.
}

func NewMessageReader(b []byte) *MessageReader {
	return &MessageReader{
		byteReader: bytes.NewBuffer(b),
		b:          b,
		i:          0,
	}
}

func (r *MessageReader) readCStringField(s *cstring) (n int, err error) {
	if *s, err = r.byteReader.ReadBytes(CSTRING_DELIM); err != nil {
		return 0, err
	}
	n = len(*s)
	r.i += int64(n)
	return
}

func (r *MessageReader) readDocField(d *Document) (n int, err error) {
	offset := r.i + int64(fieldSize(d.Size))
	// Read the document length but do not move the reader cursor.
	d.Size = int32(binary.LittleEndian.Uint32(r.b[r.i:offset]))
	d.Data = make([]byte, d.Size)

	if n, err = r.byteReader.Read(d.Data); err != nil {
		return 0, err
	}
	r.i += int64(n)
	return
}

func (r *MessageReader) readDocSliceField(docs []Document) (n int, err error) {
	var cn int
	for i := range docs {
		if cn, err = r.readDocField(&docs[i]); err != nil {
			fmt.Println("Error while reading document data:", err)
			continue
		}
		n += cn
	}
	return
}

func (r *MessageReader) readIntField(fld interface{}) (n int, err error) {
	if n = fieldSize(fld); n != 0 {
		fb := make([]byte, n)
		if _, err = r.byteReader.Read(fb); err != nil {
			return 0, err
		}
		switch fldData := fld.(type) {
		case *int32:
			*fldData = int32(binary.LittleEndian.Uint32(fb))
		case *int64:
			*fldData = int64(binary.LittleEndian.Uint64(fb))
		}
		r.i += int64(n)
	}
	return
}

func (r *MessageReader) readFields(op interface{}) (n int, err error) {
	var nRead int
	st := reflect.ValueOf(op).Elem()
	l := st.NumField()

	for i := 0; i < l; i++ {
		f := st.Field(i)
		fInterface := f.Interface()
		switch fValue := fInterface.(type) {
		case Document:
			nRead, err = r.readDocField(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case []Document:
			// Get the associated size struct field tag if present.
			if sTag := reflect.TypeOf(op).Elem().Field(i).Tag.Get("size"); sTag != "" {
				sFld := st.FieldByName(sTag).Interface()
				if size, ok := sFld.(int32); ok {
					fValue = make([]Document, size)
					nRead, err = r.readDocSliceField(fValue)
					f.Set(reflect.ValueOf(fValue))
				}
			}
			// TODO(spastorelli): Implement logic when the Document slice length is not provided by another field.
		case cstring:
			nRead, err = r.readCStringField(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case int32:
			nRead, err = r.readIntField(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case int64:
			nRead, err = r.readIntField(&fValue)
			f.Set(reflect.ValueOf(fValue))
		}
		if err != nil {
			return n, err
		}
		n += nRead
	}
	return
}

func (r *MessageReader) Read(msg *Message) (n int, err error) {
	if len(r.b) == 0 {
		return 0, nil
	}

	if n, err = r.readFields(&msg.Header); err != nil {
		return n, err
	}

	var n1 int
	switch msg.Header.OpCode {
	case OP_REPLY:
		reply := ReplyOp{}
		n1, err = r.readFields(&reply)
		msg.Op = reply
	case OP_QUERY:
		query := QueryOp{}
		n1, err = r.readFields(&query)
		msg.Op = query
	}

	n += n1
	if err != nil {
		return n, err
	}
	return
}

func (r *MessageReader) readHeader(header *MessageHeader) (n int, err error) {
	if err = binary.Read(r.byteReader, binary.LittleEndian, header); err != nil {
		fmt.Println("Error while reading Message header:", err)
		return n, err
	}
	n += binary.Size(header)
	r.i += int64(n)
	return
}
