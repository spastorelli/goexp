package ruleng

import (
	"testing"
)

func TestNumberCondition(t *testing.T) {
	inR := make(chan Literal)
	inL := make(chan Literal)
	cond := NewCondition(Lt, inL, inR)
	cond.StartEval()

	// Fix the RHS operand
	opR := &NumberLiteral{10}
	inR <- opR

	operandCondResults := map[int64]bool{
		0:  true,
		2:  true,
		4:  true,
		8:  true,
		16: false,
	}

	for oprd, expectedCondResult := range operandCondResults {
		opL := &NumberLiteral{oprd}
		t.Logf("Comparing: %v %s %v", opL, cond.op, opR)
		inL <- opL
		condResultOprd := (<-cond.Out).(*BoolLiteral)
		actualCondResult := condResultOprd.Value
		t.Logf("|___ Result: %v", actualCondResult)

		if expectedCondResult != actualCondResult {
			t.Fatalf("Condition result should be (%v) got (%v)", expectedCondResult, actualCondResult)
		}
	}
}

func TestBooleanCondition(t *testing.T) {
	inR := make(chan Literal)
	inL := make(chan Literal)
	cond := NewCondition(And, inL, inR)
	cond.StartEval()

	// Fix the RHS operand
	opR := &BoolLiteral{true}
	inR <- opR

	operandCondResults := map[bool]bool{
		true:  true,
		false: false,
	}

	for oprd, expectedCondResult := range operandCondResults {
		opL := &BoolLiteral{oprd}
		t.Logf("Comparing: %v %s %v", opL, cond.op, opR)
		inL <- opL
		condResultOprd := (<-cond.Out).(*BoolLiteral)
		actualCondResult := condResultOprd.Value
		t.Logf("|___ Result: %v", actualCondResult)

		if expectedCondResult != actualCondResult {
			t.Fatalf("Condition result should be (%v) got (%v)", expectedCondResult, actualCondResult)
		}
	}
}

func TestMultipleCondition(t *testing.T) {
	// Setup condition A
	condAInR := make(chan Literal)
	condAInL := make(chan Literal)
	condA := NewCondition(Lt, condAInL, condAInR)
	condA.StartEval()
	// Fix the RHS operand
	condAInR <- &NumberLiteral{10}

	// Setup condition B
	condBInR := make(chan Literal)
	condBInL := make(chan Literal)
	condB := NewCondition(Eq, condBInL, condBInR)
	condB.StartEval()
	// Fix the RHS operand
	condBInR <- &NumberLiteral{10}

	// Setup condition C using the output of condition A & B
	condC := NewCondition(And, condA.Out, condB.Out)
	condC.StartEval()

	condAInL <- &NumberLiteral{20}
	condBInL <- &NumberLiteral{10}

	expectedCondResult := false
	condResultOprd := (<-condC.Out).(*BoolLiteral)
	actualCondResult := condResultOprd.Value

	t.Logf("Result of combined conditions: %v", actualCondResult)
	if expectedCondResult != actualCondResult {
		t.Fatalf("Condition result should be (%v) got (%v)", expectedCondResult, actualCondResult)
	}

	// Test again changing the inputs of conditions A so final outcome is true
	condAInL <- &NumberLiteral{8}

	expectedCondResult = true
	condResultOprd = (<-condC.Out).(*BoolLiteral)
	actualCondResult = condResultOprd.Value

	t.Logf("Result of combined conditions: %v", actualCondResult)
	if expectedCondResult != actualCondResult {
		t.Fatalf("Condition result should be (%v) got (%v)", expectedCondResult, actualCondResult)
	}
}
