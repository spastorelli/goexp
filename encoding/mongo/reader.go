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

func (r *MessageReader) readCString(s *cstring) (n int, err error) {
	if *s, err = r.byteReader.ReadBytes(CStringDelim); err != nil {
		return 0, err
	}
	n = len(*s)
	r.i += int64(n)
	return
}

func (r *MessageReader) readDocument(d *Document) (n int, err error) {
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

func (r *MessageReader) readDocuments(docs []Document) (n int, err error) {
	var cn int
	for i := range docs {
		if cn, err = r.readDocument(&docs[i]); err != nil {
			fmt.Println("Error while reading document data:", err)
			continue
		}
		n += cn
	}
	return
}

func (r *MessageReader) readData(data interface{}) (n int, err error) {
	st := reflect.ValueOf(data).Elem()
	l := st.NumField()

	for i := 0; i < l; i++ {
		nRead := 0
		f := st.Field(i)
		fInterface := f.Interface()
		switch fValue := fInterface.(type) {
		case Document:
			nRead, err = r.readDocument(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case []Document:
			// Get the associated size struct field tag if present.
			if sTag := reflect.TypeOf(data).Elem().Field(i).Tag.Get("size"); sTag != "" {
				sFld := st.FieldByName(sTag).Interface()
				if size, ok := sFld.(int32); ok {
					fValue = make([]Document, size)
					nRead, err = r.readDocuments(fValue)
					f.Set(reflect.ValueOf(fValue))
				}
			} else {
				var cn int
				// TODO(spastorelli): Choose a better initial capacity value.
				fValue := make([]Document, 0, 20)

				// Read the docs until it reaches the end of the message bytes.
				for i := 0; r.byteReader.Len() != 0; i++ {
					fValue = append(fValue, Document{})
					cn, err = r.readDocument(&fValue[i])
					nRead += cn
				}
				f.Set(reflect.ValueOf(fValue))
			}
		case cstring:
			nRead, err = r.readCString(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case int32:
			nRead, err = r.readInt(&fValue)
			f.Set(reflect.ValueOf(fValue))
		case int64:
			nRead, err = r.readInt(&fValue)
			f.Set(reflect.ValueOf(fValue))
		}
		if err != nil {
			return n, err
		}
		n += nRead
	}
	return
}

func (r *MessageReader) readInt(fld interface{}) (n int, err error) {
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

func (r *MessageReader) Read(msg *Message) (n int, err error) {
	if len(r.b) == 0 {
		return 0, nil
	}

	if n, err = r.readData(&msg.Header); err != nil {
		return n, err
	}

	var n1 int
	switch msg.Header.OpCode {
	case OpReply:
		reply := ReplyOp{}
		n1, err = r.readData(&reply)
		msg.Op = reply
	case OpQuery:
		query := QueryOp{}
		n1, err = r.readData(&query)
		msg.Op = query
	}

	n += n1
	if err != nil {
		return n, err
	}
	return
}
