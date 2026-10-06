package service

import (
	"testing"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

func TestAssembleExpensesData_PaymentImpacts(t *testing.T) {
	// A payment usually settles a commitment issued on an earlier day, so the
	// commitment is not part of the batch. The impact must still reach the
	// payment it belongs to.
	payments := []model.Payment{
		{PaymentCode: "158454264152025OB000828", ManagementUnitCode: 158454, ManagementUnitName: "CAMPUS TRES LAGOAS"},
		{PaymentCode: "158454264152025OB000829", ManagementUnitCode: 158454},
	}
	paImpacts := []model.PaymentImpactedCommitment{
		{PaymentCode: "158454264152025OB000828", CommitmentCode: "158454264152025NE000013", PaidValueBRL: 27169.90},
		{PaymentCode: "158454264152025OB000828", CommitmentCode: "158454264152024NE000200", PaidValueBRL: 100},
		{PaymentCode: "158454264152025OB000829", CommitmentCode: "158454264152025NE000013", PaidValueBRL: 50},
		// Impact of a payment that is not in the batch: nowhere to attach it.
		{PaymentCode: "999999264152025OB000001", CommitmentCode: "999999264152025NE000001", PaidValueBRL: 1},
	}

	units := AssembleExpensesData(nil, nil, nil, nil, nil, payments, paImpacts)

	unit, ok := units["158454"]
	if !ok {
		t.Fatalf("unit 158454 missing; got units %v", keys(units))
	}
	if len(units) != 1 {
		t.Errorf("got %d units, want 1: %v", len(units), keys(units))
	}
	if len(unit.Payments) != 2 {
		t.Fatalf("got %d payments, want 2", len(unit.Payments))
	}

	want := map[string][]string{
		"158454264152025OB000828": {"158454264152025NE000013", "158454264152024NE000200"},
		"158454264152025OB000829": {"158454264152025NE000013"},
	}
	for _, p := range unit.Payments {
		var got []string
		for _, imp := range p.ImpactedCommitments {
			if imp.PaymentCode != p.PaymentCode {
				t.Errorf("payment %s holds impact of payment %s", p.PaymentCode, imp.PaymentCode)
			}
			got = append(got, imp.CommitmentCode)
		}
		if len(got) != len(want[p.PaymentCode]) {
			t.Errorf("payment %s: got commitments %v, want %v", p.PaymentCode, got, want[p.PaymentCode])
			continue
		}
		for i := range got {
			if got[i] != want[p.PaymentCode][i] {
				t.Errorf("payment %s: got commitments %v, want %v", p.PaymentCode, got, want[p.PaymentCode])
				break
			}
		}
	}
}

func keys(m map[string]*UnitsExpenses) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
