package converter

import (
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
	"github.com/go-gota/gota/dataframe"
)

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func DfRowToCommitment(df dataframe.DataFrame, rowIdx int) types.Commitment {
	getStr := func(col string) string {
		if idx := df.Names(); containsString(idx, col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return types.Commitment{
		CommitmentCode:                getStr("Código Empenho"),
		ResumedCommitmentCode:         getStr("Código Empenho Resumido"),
		EmitionDate:                   getStr("Data Emissão"),
		Type:                          getStr("Tipo Empenho"),
		ManagementCode:                getStr("Código Gestão"),
		ManagementName:                getStr("Gestão"),
		Process:                       getStr("Processo"),
		FavoredName:                   getStr("Favorecido"),
		FavoredCode:                   getStr("Código Favorecido"),
		ExpenseNature:                 getStr("Elemento de Despesa"),
		CompleteExpenseNature:         getStr("Natureza de Despesa Completa"),
		BudgetPlan:                    getStr("Plano Orçamentário"),
		CommitmentOriginalValue:       getStr("Valor Original do Empenho"),
		CommitmentValueConvertedToBRL: getStr("Valor do Empenho Convertido pra R$"),
		ConversionValueUsed:           getStr("Valor Utilizado na Conversão"),
		Items:                         []types.CommitmentItem{},
	}
}

func DfRowToLiquidation(df dataframe.DataFrame, rowIdx int) types.Liquidation {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return types.Liquidation{
		LiquidationCode:        getStr("Código Liquidação"),
		LiquidationCodeResumed: getStr("Código Liquidação Resumido"),
		LiquidationEmitionDate: getStr("Data Emissão"),
		DocumentCodeType:       getStr("Código Tipo Documento"),
		DocumentType:           getStr("Tipo Documento"),
		FavoredCode:            getStr("Código Favorecido"),
		FavoredName:            getStr("Favorecido"),
		Observation:            getStr("Observação"),
	}
}

func DfRowToPayment(df dataframe.DataFrame, rowIdx int) types.Payment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return types.Payment{
		PaymentCode:           getStr("Código Pagamento"),
		PaymentCodeResumed:    getStr("Código Pagamento Resumido"),
		PaymentEmitionDate:    getStr("Data Emissão"),
		DocumentCodeType:      getStr("Código Tipo Documento"),
		DocumentType:          getStr("Tipo Documento"),
		FavoredCode:           getStr("Código Favorecido"),
		FavoredName:           getStr("Favorecido"),
		ExtraBudgetary:        getStr("Extraorçamentário"),
		Process:               getStr("Processo"),
		OriginalPaymentValue:  getStr("Valor Original do Pagamento"),
		ConvertedPaymentValue: getStr("Valor do Pagamento Convertido pra R$"),
		ConversionUsedValue:   getStr("Valor Utilizado na Conversão"),
	}
}

func DfRowToCommitmentItem(df dataframe.DataFrame, rowIdx int) types.CommitmentItem {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	getInt := func(col string) int {
		if idx := df.Names(); containsString(idx, col) {
			val, err := df.Col(col).Elem(rowIdx).Int()
			if err != nil {
				return 0
			}
			return val
		}
		return 0
	}

	return types.CommitmentItem{
		ExpenseCategory:    getStr("Categoria de Despesa"),
		ExpenseGroup:       getStr("Grupo de Despesa"),
		AplicationModality: getStr("Modalidade de Aplicação"),
		ExpenseElement:     getStr("Elemento de Despesa"),
		Description:        getStr("Descrição"),
		Sequential:         getInt("Sequencial"),
		Quantity:           getStr("Quantidade"),
		UnitPrice:          getStr("Valor Unitário"),
		CurrentValue:       getStr("Valor Atual"),
		TotalPrice:         getStr("Valor Total"),
		History:            []types.CommitmentItemHistory{},
	}
}

func DfRowToCommitmentItemHistory(df dataframe.DataFrame, rowIdx int) types.CommitmentItemHistory {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	getInt := func(col string) int {
		if idx := df.Names(); containsString(idx, col) {
			val, err := df.Col(col).Elem(rowIdx).Int()
			if err != nil {
				return 0
			}
			return val
		}
		return 0
	}

	return types.CommitmentItemHistory{
		OperationType:  getStr("Tipo Operação"),
		OperationDate:  getStr("Data Operação"),
		Sequential:     getInt("Sequencial"),
		ItemQuantity:   getStr("Quantidade Item"),
		ItemUnitPrice:  getStr("Valor Unitário Item"),
		ItemTotalPrice: getStr("Valor Total Item"),
	}
}

func DfRowToPaymentImpactedCommitment(df dataframe.DataFrame, rowIdx int) types.PaymentImpactedCommitment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	return types.PaymentImpactedCommitment{
		CommitmentCode:             getStr("Código Empenho"),
		CompleteExpenseNature:      getStr("Natureza de Despesa Completa"),
		Subitem:                    getStr("Subitem"),
		PaidValueBRL:               getStr("Valor Pago (R$)"),
		RegisteredPayablesValueBRL: getStr("Valor Restos a Pagar Inscritos (R$)"),
		CanceledPayablesValueBRL:   getStr("Valor Restos a Pagar Cancelado (R$)"),
		OutstandingValuePaidBRL:    getStr("Valor Restos a Pagar Pagos (R$)"),
	}
}

func DfRowToLiquidationImpactedCommitment(df dataframe.DataFrame, rowIdx int) types.LiquidationImpactedCommitment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	return types.LiquidationImpactedCommitment{
		CommitmentCode:                getStr("Código Empenho"),
		CompleteExpenseNature:         getStr("Natureza de Despesa Completa"),
		Subitem:                       getStr("Subitem"),
		LiquidatedValueBRL:            getStr("Valor Liquidado (R$)"),
		RegisteredPayablesValueBRL:    getStr("Valor Restos a Pagar Inscritos (R$)"),
		CanceledPayablesValueBRL:      getStr("Valor Restos a Pagar Cancelado (R$)"),
		OutstandingValueLiquidatedBRL: getStr("Valor Restos a Pagar Pagos (R$)"),
	}
}
