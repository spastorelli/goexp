package mongo

import (
	"testing"
)

var (
	oneDocReplyBuf = []byte{
		// Message Header
		0x66, 0x00, 0x00, 0x00, // MessageLength: 102
		0xd2, 0x04, 0x00, 0x00, // RequestId: 1234
		0x01, 0x00, 0x00, 0x00, // ResponseTo: 1
		0x01, 0x00, 0x00, 0x00, // OpCode: 1 (OP_REPLY)
		// Reply Operation
		0x08, 0x00, 0x00, 0x00, // Flags: 1000
		0x00, 0x00, 0x00, 0x00, // CursorId: 0
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, // StartingFrom: 0
        0x01, 0x00, 0x00, 0x00, // NumReturned: 1
		// BSON Document
        0x42, 0x00, 0x00, 0x00, 0x07, 0x5f, 0x69, 0x64,
        0x00, 0x56, 0xcb, 0x21, 0x70, 0xd8, 0x45, 0x44,
        0xa7, 0x07, 0x94, 0x5d, 0xdf, 0x01, 0x61, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x01, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x01, 0x63, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x01, 0x64,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
	}
	mulDocReplyBuf = []byte{
		// Message Header
		0xa8, 0x00, 0x00, 0x00, // MessageLength: 168
		0xd2, 0x04, 0x00, 0x00, // RequestId: 1234
		0x01, 0x00, 0x00, 0x00, // ResponseTo: 1
		0x01, 0x00, 0x00, 0x00, // OpCode: 1 (OP_REPLY)
		// Reply Operation
		0x08, 0x00, 0x00, 0x00, // Flags: 1000
		0x00, 0x00, 0x00, 0x00, // CursorId: 0
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, // StartingFrom: 0
        0x02, 0x00, 0x00, 0x00, // NumReturned: 2
		// BSON Document #1
        0x42, 0x00, 0x00, 0x00, 0x07, 0x5f, 0x69, 0x64,
        0x00, 0x56, 0xcb, 0x21, 0x70, 0xd8, 0x45, 0x44,
        0xa7, 0x07, 0x94, 0x5d, 0xdf, 0x01, 0x61, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x01, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x01, 0x63, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x01, 0x64,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
		// BSON Document #2
        0x42, 0x00, 0x00, 0x00, 0x07, 0x5f, 0x69, 0x64,
        0x00, 0x56, 0xcb, 0x21, 0x70, 0xd8, 0x45, 0x44,
        0xa7, 0x07, 0x94, 0x5d, 0xdf, 0x01, 0x61, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x01, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x40, 0x01, 0x63, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x01, 0x64,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
	}
    testData = []struct {
        ExpectedMessage Message
        MessageBytes []byte
    }{
        {
            ExpectedMessage: Message{
                Header: MessageHeader{
                    MessageLength: 102, RequestId: 1234,
                    ResponseTo: 1, OpCode: OP_REPLY,
                },
                Op: ReplyOp{CursorId: 0, StartingFrom:0, NumReturned: 1},
            },
            MessageBytes: oneDocReplyBuf,
        },
        {
            ExpectedMessage: Message{
                Header: MessageHeader{
                    MessageLength: 168, RequestId: 1234,
                    ResponseTo: 1, OpCode: OP_REPLY,
                },
                Op: ReplyOp{CursorId: 0, StartingFrom:0, NumReturned: 2},
            },
            MessageBytes: mulDocReplyBuf,
        },
    }
)

func TestReadReplyMessage(t *testing.T) {
    var reader *MessageReader
    var actualMsg Message
    var tRead int
    var err error

    for _, tReply := range testData {
        reader = NewMessageReader(tReply.MessageBytes)
        actualMsg = Message{}
        if tRead, err = reader.Read(&actualMsg); err != nil {
            t.Fatal("Error while reading Mongo Message:", err)
        }

        expectedRequestId := tReply.ExpectedMessage.Header.RequestId
        actualRequestId := actualMsg.Header.RequestId
        if actualRequestId != expectedRequestId {
            t.Fatalf("Message header RequestId should be %d. Got: %d", expectedRequestId, actualRequestId)
        }

        expectedOpCode := tReply.ExpectedMessage.Header.OpCode
        actualOpCode := actualMsg.Header.OpCode
        if actualOpCode != expectedOpCode {
            t.Fatalf("Message header OpCode should be %d. Got: %d", expectedOpCode, actualOpCode)
        }
        expectedMessageLength := tReply.ExpectedMessage.Header.MessageLength
        actualMessageLength := actualMsg.Header.MessageLength
        if actualMessageLength != expectedMessageLength {
            t.Fatalf("Message length should be %d. Got: %d", expectedMessageLength, actualMessageLength)
        }
        msgLength := int(actualMessageLength)
        if msgLength != tRead {
            t.Fatalf("Message length (%d) does not match the number of bytes read by reader (%d)", msgLength, tRead)
        }

        actualReply, ok := actualMsg.Op.(ReplyOp)
        if !ok {
            t.Fatalf("Message operation should be ReplyOp. Got: %s", actualReply)
        }

        expectedNumReturned := tReply.ExpectedMessage.Op.(ReplyOp).NumReturned
        actualNumReturned := actualReply.NumReturned
        if actualNumReturned !=  expectedNumReturned {
            t.Fatalf("Number of docs returned should be %d. Got: %d", expectedNumReturned, actualNumReturned)
        }
        actualDocsReturned := int32(len(actualReply.Docs))
        if actualDocsReturned !=  expectedNumReturned {
            t.Fatalf("Number of docs returned should be %d. Got: %d", expectedNumReturned, actualDocsReturned)
        }
        t.Log(actualMsg)
    }
}