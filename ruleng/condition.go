package ruleng

import (
	"strconv"
	"sync"
)

const (
	Lt Operator = iota
	Lte
	Eq
	Gt
	Gte
	And
	Or
)

type Operator uint8

func (opr Operator) String() string {
	switch opr {
	case Lt:
		return "<"
	case Lte:
		return "<="
	case Eq:
		return "=="
	case Gte:
		return ">="
	case Gt:
		return ">"
	case And:
		return "&&"
	case Or:
		return "||"
	default:
		return "Unknown"
	}
}

type Literal interface {
	Compare(opr Operator, rOprd Literal) bool
}

type NumberLiteral struct {
	Value int64
}

func (l *NumberLiteral) Compare(opr Operator, rLit Literal) bool {
	r := rLit.(*NumberLiteral)
	switch opr {
	case Lt:
		return l.Value < r.Value
	case Lte:
		return l.Value <= r.Value
	case Eq:
		return l.Value == r.Value
	case Gte:
		return l.Value >= r.Value
	case Gt:
		return l.Value > r.Value
	default:
		// TODO(spastorelli): Return an unsupported operator error instead
		return false
	}
}

func (l *NumberLiteral) String() string {
	return strconv.FormatInt(l.Value, 10)
}

type BoolLiteral struct {
	Value bool
}

func (l *BoolLiteral) Compare(opr Operator, rLit Literal) bool {
	r := rLit.(*BoolLiteral)
	switch opr {
	case And:
		return l.Value && r.Value
	case Or:
		return l.Value || r.Value
	default:
		// TODO(spastorelli): Return an unsupported operator error instead
		return false
	}
}

func (l *BoolLiteral) String() string {
	return strconv.FormatBool(l.Value)
}

type Condition struct {
	op  Operator
	inL chan Literal
	inR chan Literal
	Out chan Literal
	end chan bool
	wg  sync.WaitGroup
}

func NewCondition(op Operator, inL, inR chan Literal) *Condition {
	return &Condition{
		op:  op,
		inL: inL,
		inR: inR,
		Out: make(chan Literal),
		end: make(chan bool),
	}
}

func (c *Condition) eval() {
	var lOperand, rOperand Literal
	for {
		select {
		case v := <-c.inL:
			lOperand = v
		case v := <-c.inR:
			rOperand = v
		case <-c.end:
			break
		}
		if lOperand != nil && rOperand != nil {
			result := lOperand.Compare(c.op, rOperand)
			c.Out <- &BoolLiteral{result}
		}
	}
	c.wg.Done()
	c.terminate()
}

func (c *Condition) terminate() {
	c.wg.Wait()
	close(c.inL)
	close(c.inR)
}

func (c *Condition) StartEval() {
	c.wg.Add(1)
	go c.eval()
}

func (c *Condition) StopEval() {
	c.end <- true
}
