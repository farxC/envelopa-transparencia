package portal

import (
	"math"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
)

var budgetHeader = []string{
	"EXERCÍCIO", "CÓDIGO ÓRGÃO SUPERIOR", "NOME ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO SUBORDINADO", "NOME ÓRGÃO SUBORDINADO",
	"CÓDIGO UNIDADE ORÇAMENTÁRIA", "NOME UNIDADE ORÇAMENTÁRIA", "CÓDIGO FUNÇÃO", "NOME FUNÇÃO", "CÓDIGO SUBFUNÇÃO", "NOME SUBFUNÇÃO",
	"CÓDIGO PROGRAMA ORÇAMENTÁRIO", "NOME PROGRAMA ORÇAMENTÁRIO", "CÓDIGO AÇÃO", "NOME AÇÃO",
	"CÓDIGO CATEGORIA ECONÔMICA", "NOME CATEGORIA ECONÔMICA", "CÓDIGO GRUPO DE DESPESA", "NOME GRUPO DE DESPESA",
	"CÓDIGO ELEMENTO DE DESPESA", "NOME ELEMENTO DE DESPESA",
	"ORÇAMENTO INICIAL (R$)", "ORÇAMENTO ATUALIZADO (R$)", "ORÇAMENTO EMPENHADO (R$)", "ORÇAMENTO REALIZADO (R$)",
	"% REALIZADO DO ORÇAMENTO (COM RELAÇÃO AO ORÇAMENTO ATUALIZADO)",
}

func budgetRow(updated, executed, percent string) []string {
	return []string{
		"2025", "26000", "Ministério da Educação", "26421", "IFRO", "26421", "IFRO", "12", "Educação", "363", "Ensino profissional",
		"5112", "Educação Profissional", "20RL", "Funcionamento", "3", "Despesas Correntes", "3", "Outras Despesas Correntes",
		"39", "Outros Serviços de Terceiros - PJ", "1.000,00", updated, "800,00", executed, percent,
	}
}

func TestDfRowToExpenseBudget_PercentIsComputed(t *testing.T) {
	tests := []struct {
		name              string
		updated, executed string
		percentInFile     string
		want              float64
	}{
		{"percent present", "2.000,00", "500,00", "25,00%", 25},
		{"percent blank", "2.000,00", "500,00", "", 25},
		{"percent dash", "2.000,00", "500,00", "-", 25},
		{"percent not a number", "2.000,00", "500,00", "Infinity", 25},
		{"updated budget zero", "0,00", "500,00", "", 0},
		// Above the old NUMERIC(7,4) limit of 999.9999.
		{"executed far above updated", "10,00", "500,00", "5000,00%", 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := dataframe.LoadRecords([][]string{budgetHeader, budgetRow(tt.updated, tt.executed, tt.percentInFile)}, dataframe.DetectTypes(false))
			got, err := DfRowToExpenseBudget(df, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if math.Abs(got.PercentExecutedBudget-tt.want) > 1e-9 {
				t.Errorf("PercentExecutedBudget = %v, want %v", got.PercentExecutedBudget, tt.want)
			}
			if got.ActionCode != "20RL" || got.ExpenseElementCode != 39 || got.SubordinateAgencyCode != 26421 {
				t.Errorf("unexpected keys: action=%q element=%d agency=%d", got.ActionCode, got.ExpenseElementCode, got.SubordinateAgencyCode)
			}
		})
	}
}

func TestDfRowToExpenseBudget_InvalidMoneyHasRowContext(t *testing.T) {
	df := dataframe.LoadRecords([][]string{budgetHeader, budgetRow("abc", "500,00", "")}, dataframe.DetectTypes(false))
	_, err := DfRowToExpenseBudget(df, 0)
	if err == nil {
		t.Fatal("expected error for an invalid money value")
	}
	for _, want := range []string{"ORÇAMENTO ATUALIZADO", "action=20RL", "element=39"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}
