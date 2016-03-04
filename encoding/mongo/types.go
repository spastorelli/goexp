package mongo

import "fmt"

// The wire protocol operation codes.
const (
	OP_REPLY        = 1
	OP_MSG          = 1000 // Deprecated
	OP_UPDATE       = 2001
	OP_INSERT       = 2002
	RESERVED        = 2003
	OP_QUERY        = 2004
	OP_GET_MORE     = 2005
	OP_DELETE       = 2006
	OP_KILL_CURSORS = 2007
)

type UnmarshalDocFunc func([]byte, interface{}) error

// Document defines the structure to hold a BSON document size and its raw data.
type Document struct {
	Size int32
	Data []byte
}

func (doc *Document) Unmarshal(unmarshal UnmarshalDocFunc, out interface{}) (err error) {
	return unmarshal(doc.Data, out)
}

func (doc Document) String() string {
	f := "Document {Size: %d}"
	return fmt.Sprintf(f, doc.Size)
}

const CSTRING_DELIM = 0x00

type cstring []byte

// Message defines the structure of a MongoDB wire protocol message.
type Message struct {
	Header MessageHeader
	Op     interface{}
}

func (m Message) String() string {
	f := `
        Message {
            Header: %v
            Op: %v
        }
    `
	return fmt.Sprintf(f, m.Header, m.Op)
}

// MessageHeader defines the header of a MongoDB wire protocol messages.
type MessageHeader struct {
	MessageLength int32 // The total size of the message in bytes, including these 4 bytes.
	RequestId     int32 // The unique identifier of the message.
	ResponseTo    int32 // The identifier from the original request (used in responses from DB).
	OpCode        int32 // The operation code defining the request type.
}

func (h MessageHeader) String() string {
	f := `{
                MessageLength: %d
                RequestId: %d
                ResponseTo: %d
                OpCode: %d
            }`
	return fmt.Sprintf(f, h.MessageLength, h.RequestId, h.ResponseTo, h.OpCode)
}

// ReplyOp defines the structure of a reply operation (OpCode: OP_REPLY).
type ReplyOp struct {
	Flags        int32
	CursorId     int64
	StartingFrom int32
	NumReturned  int32
	Docs         []Document `size:"NumReturned"`
}

func (r ReplyOp) String() string {
	f := `(ReplyOp) {
                Flags: %b
                CursorId: %d
                StartingFrom: %d
                NumReturned: %d
                Docs: %v
            }`

	return fmt.Sprintf(f, r.Flags, r.CursorId, r.StartingFrom, r.NumReturned, r.Docs)
}

// QueryOp defines the structure of a query operation (OpCode: OP_QUERY).
type QueryOp struct {
	Flags          int32    // The bit vector defining the options for the query operation.
	CollectionName cstring  // The full collection name e.g "db.collection".
	NumToSkip      int32    // The number of document to skip.
	NumToReturn    int32    // The numer of document to return in the first OP_REPLY batch.
	Doc            Document // The query object document.
	Projections    Document // The field projections document.
}

func (q QueryOp) String() string {
	f := `(QueryOp) {
                Flags: %b
                CollectionName: %s
                NumToSkip: %d
                NumToReturn: %d
                Doc: %v
                Projections: %v
            }`

	return fmt.Sprintf(f, q.Flags, q.CollectionName, q.NumToSkip, q.NumToReturn, q.Doc, q.Projections)
}

type UpdateOp struct {
	_              int32    // Reserved for future use.
	CollectionName cstring  // The full collection name e.g "db.collection".
	Flags          int32    // The bit vector defining the options for the update operation.
	Doc            Document // The document holding the specification of the update to perform.
	SelectorDoc    Document // The query to select the documents to update.
}

type InsertOp struct {
	Flags          int32      // The bit vector defining the options for the insert operation.
	CollectionName cstring    // The full collection name e.g "db.collection".
	Docs           []Document // The documents to insert.
}

type GetMoreOp struct {
	_              int32   // Reserved for future use.
	CollectionName cstring // The full collection name e.g "db.collection".
	numberToReturn int32   // The numer of document to return in this batch.
	CursorID       int64   // The identifier of the cursor from the OP_REPLY operation.
}

type DeleteOp struct {
	_              int32    // Reserved for future use.
	CollectionName cstring  // The full collection name e.g "db.collection".
	Flags          int32    // The bit vector defining the options for the delete operation.
	SelectorDoc    Document // The query to select the documents to delete.
}
