package mongo

// Types definitions of the operation messages that are used in the
// MongoDB Wire Protocol. More detailed information:
// https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/#messages-types-and-formats

import "fmt"

// The wire protocol operation codes.
const (
	OpReply       = 1
	OpMsg         = 1000 // deprecated
	OpUpdate      = 2001
	OpInsert      = 2002
	Reserved      = 2003
	OpQuery       = 2004
	OpGetMore     = 2005
	OpDelete      = 2006
	OpKillCursors = 2007
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

const CStringDelim = 0x00

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

// MessageHeader defines the header of a MongoDB wire protocol message.
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

// ReplyOp defines the structure of a reply operation message (OpCode: OP_REPLY).
// A reply operation message is sent by the database in response to a query or get_more
// operation message.
type ReplyOp struct {
	Flags        int32      // The bit vector used to specify the operation flags.
	CursorId     int64      // The cursor ID for subsequents OP_GET_MORE operations.
	StartingFrom int32      // The position in the cursor the reply operation is starting.
	NumReturned  int32      // The number of documents in the reply.
	Docs         []Document `size:"NumReturned"` // The documents of the reply operation.
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

// QueryOp defines the structure of a query operation message (OpCode: OP_QUERY).
// A query operation message is used to query the database for documents in a collection.
type QueryOp struct {
	Flags          int32    // The bit vector used to specify the operation flags.
	CollectionName cstring  // The full collection name e.g "db.collection".
	NumToSkip      int32    // The number of documents to skip.
	NumToReturn    int32    // The numer of documents to return in the first OP_REPLY batch.
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

// UpdateOp defines the structure of an update operation message (OpCode: OP_UPDATE).
// An update operation message is used to update a document in a collection.
type UpdateOp struct {
	_              int32    // Reserved for future use.
	CollectionName cstring  // The full collection name e.g "db.collection".
	Flags          int32    // The bit vector used to specify the operation flags.
	SelectorDoc    Document // The query to select the document(s) to update.
	Doc            Document // The document holding the specification of the update to perform.
}

// InsertOp defines the structure of an insert operation message (OpCode: OP_INSERT).
// An insert operation message is used to insert one or more documents in a collection.
type InsertOp struct {
	Flags          int32      // The bit vector used to specify the operation flags.
	CollectionName cstring    // The full collection name e.g "db.collection".
	Docs           []Document // The document(s) to insert.
}

// GetMoreOp defines the structure of a get_more operation message (OpCode: OP_GET_MORE).
// A get_more operation message is used to query the database for more documents in a collection
// following a related query operation.
type GetMoreOp struct {
	_              int32   // Reserved for future use.
	CollectionName cstring // The full collection name e.g "db.collection".
	NumToReturn    int32   // The number of documents to return in this OP_REPLY batch.
	CursorID       int64   // The cursor ID from the related OP_REPLY operation.
}

// DeleteOp defines the structure of a delete operation message (OpCode: OP_DELETE).
// A delete operation message is used to delete one or more documents from a collection.
type DeleteOp struct {
	_              int32    // Reserved for future use.
	CollectionName cstring  // The full collection name e.g "db.collection".
	Flags          int32    // The bit vector used to specify the operation flags.
	SelectorDoc    Document // The query to select the document(s) to delete.
}
