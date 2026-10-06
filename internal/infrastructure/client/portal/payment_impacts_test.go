package portal

import (
	"reflect"
	"testing"

	"github.com/go-gota/gota/dataframe"
)

var paymentImpactsHeader = []string{
	"Código Pagamento", "Código Empenho", "Código Natureza Despesa Completa", "Subitem",
	"Valor Pago (R$)", "Valor Restos a Pagar Inscritos (R$)", "Valor Restos a Pagar Cancelado (R$)", "Valor Restos a Pagar Pagos (R$)",
}

func TestMatchPaymentImpacts(t *testing.T) {
	// The day's payments of the unit. OB000828 settles a commitment issued
	// months earlier, which is not among the day's commitments.
	payments := dataframe.LoadRecords([][]string{
		{"Código Pagamento", "Código Unidade Gestora"},
		{"158454264152025OB000828", "158454"},
		{"158454264152025OB000829", "158454"},
	})
	impacts := dataframe.LoadRecords([][]string{
		paymentImpactsHeader,
		{"158454264152025OB000828", "158454264152025NE000013", "33903702", "LIMPEZA E CONSERVACAO", "27169,90", "0,00", "0,00", "0,00"},
		{"158454264152025OB000829", "158454264152024NE000200", "33903917", "MANUT. E CONSERV. DE MAQUINAS", "3550,00", "0,00", "0,00", "0,00"},
		// Another unit's payment of a commitment of this unit must not match:
		// impacts follow the payment, not the commitment.
		{"999999264152025OB000001", "158454264152025NE000013", "33903702", "LIMPEZA E CONSERVACAO", "1,00", "0,00", "0,00", "0,00"},
		{"999999264152025OB000002", "999999264152025NE000001", "33903702", "LIMPEZA E CONSERVACAO", "1,00", "0,00", "0,00", "0,00"},
	}, dataframe.DetectTypes(false))

	got := matchPaymentImpacts(impacts, payments, false)

	want := []string{"158454264152025OB000828", "158454264152025OB000829"}
	if got.Nrow() != len(want) {
		t.Fatalf("got %d rows, want %d: %v", got.Nrow(), len(want), got)
	}
	if codes := got.Col("Código Pagamento").Records(); !reflect.DeepEqual(codes, want) {
		t.Errorf("got payment codes %v, want %v", codes, want)
	}
}

func TestMatchPaymentImpacts_NoPayments(t *testing.T) {
	impacts := dataframe.LoadRecords([][]string{
		paymentImpactsHeader,
		{"158454264152025OB000828", "158454264152025NE000013", "33903702", "LIMPEZA", "1,00", "0,00", "0,00", "0,00"},
	}, dataframe.DetectTypes(false))

	if got := matchPaymentImpacts(impacts, dataframe.New(), false); got.Nrow() != 0 {
		t.Errorf("got %d rows, want 0", got.Nrow())
	}
}
