package converter

import (
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
	"github.com/farxc/transparency_wrapper/internal/transparency/utils"
	"github.com/go-gota/gota/dataframe"
)

func DfRowToCommitment(df dataframe.DataFrame, rowIdx int) types.Commitment {

	return types.Commitment{
		CommitmentId:                  utils.GetStr("Id Empenho", rowIdx, &df),
		CommitmentCode:                utils.GetStr("Código Empenho", rowIdx, &df),
		DocumentCodeType:              utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:                  utils.GetStr("Tipo Documento", rowIdx, &df),
		ResumedCommitmentCode:         utils.GetStr("Código Empenho Resumido", rowIdx, &df),
		EmitionDate:                   utils.GetStr("Data Emissão", rowIdx, &df),
		Type:                          utils.GetStr("Tipo Empenho", rowIdx, &df),
		ManagementUnitName:            utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:            utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:                utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:                utils.GetStr("Gestão", rowIdx, &df),
		Process:                       utils.GetStr("Processo", rowIdx, &df),
		FavoredName:                   utils.GetStr("Favorecido", rowIdx, &df),
		FavoredCode:                   utils.GetStr("Código Favorecido", rowIdx, &df),
		ExpenseNature:                 utils.GetStr("Elemento de Despesa", rowIdx, &df),
		CompleteExpenseNature:         utils.GetStr("Natureza de Despesa Completa", rowIdx, &df),
		BudgetPlan:                    utils.GetStr("Plano Orçamentário", rowIdx, &df),
		CommitmentOriginalValue:       utils.GetStr("Valor Original do Empenho", rowIdx, &df),
		CommitmentValueConvertedToBRL: utils.GetStr("Valor do Empenho Convertido pra R$", rowIdx, &df),
		ConversionValueUsed:           utils.GetStr("Valor Utilizado na Conversão", rowIdx, &df),
		Items:                         []types.CommitmentItem{},
	}
}

func DfRowToLiquidation(df dataframe.DataFrame, rowIdx int) types.Liquidation {

	return types.Liquidation{
		LiquidationCode:        utils.GetStr("Código Liquidação", rowIdx, &df),
		LiquidationCodeResumed: utils.GetStr("Código Liquidação Resumido", rowIdx, &df),
		LiquidationEmitionDate: utils.GetStr("Data Emissão", rowIdx, &df),
		ManagementUnitName:     utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:     utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:         utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:         utils.GetStr("Gestão", rowIdx, &df),
		DocumentCodeType:       utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:           utils.GetStr("Tipo Documento", rowIdx, &df),
		FavoredCode:            utils.GetStr("Código Favorecido", rowIdx, &df),
		FavoredName:            utils.GetStr("Favorecido", rowIdx, &df),
		Observation:            utils.GetStr("Observação", rowIdx, &df),
	}
}

func DfRowToPayment(df dataframe.DataFrame, rowIdx int) types.Payment {

	return types.Payment{
		PaymentCode:           utils.GetStr("Código Pagamento", rowIdx, &df),
		PaymentCodeResumed:    utils.GetStr("Código Pagamento Resumido", rowIdx, &df),
		PaymentEmitionDate:    utils.GetStr("Data Emissão", rowIdx, &df),
		ManagementUnitName:    utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:    utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:        utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:        utils.GetStr("Gestão", rowIdx, &df),
		DocumentCodeType:      utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:          utils.GetStr("Tipo Documento", rowIdx, &df),
		Observation:           utils.GetStr("Observação", rowIdx, &df),
		FavoredCode:           utils.GetStr("Código Favorecido", rowIdx, &df),
		FavoredName:           utils.GetStr("Favorecido", rowIdx, &df),
		ExtraBudgetary:        utils.GetStr("Extraorçamentário", rowIdx, &df),
		Process:               utils.GetStr("Processo", rowIdx, &df),
		OriginalPaymentValue:  utils.GetStr("Valor Original do Pagamento", rowIdx, &df),
		ConvertedPaymentValue: utils.GetStr("Valor do Pagamento Convertido pra R$", rowIdx, &df),
		ConversionUsedValue:   utils.GetStr("Valor Utilizado na Conversão", rowIdx, &df),
	}
}

func DfRowToCommitmentItem(df dataframe.DataFrame, rowIdx int) types.CommitmentItem {

	return types.CommitmentItem{
		CommitmentId:       utils.GetStr("Id Empenho", rowIdx, &df),
		CommitmentCode:     utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseCategory:    utils.GetStr("Categoria de Despesa", rowIdx, &df),
		ExpenseGroup:       utils.GetStr("Grupo de Despesa", rowIdx, &df),
		AplicationModality: utils.GetStr("Modalidade de Aplicação", rowIdx, &df),
		ExpenseElement:     utils.GetStr("Elemento de Despesa", rowIdx, &df),
		Description:        utils.GetStr("Descrição", rowIdx, &df),
		Sequential:         utils.GetInt("Sequencial", rowIdx, &df),
		Quantity:           utils.GetStr("Quantidade", rowIdx, &df),
		UnitPrice:          utils.GetStr("Valor Unitário", rowIdx, &df),
		CurrentValue:       utils.GetStr("Valor Atual", rowIdx, &df),
		TotalPrice:         utils.GetStr("Valor Total", rowIdx, &df),
		History:            []types.CommitmentItemHistory{},
	}
}

func DfRowToCommitmentItemHistory(df dataframe.DataFrame, rowIdx int) types.CommitmentItemHistory {
	return types.CommitmentItemHistory{
		CommitmentId:   utils.GetStr("Id Empenho", rowIdx, &df),
		CommitmentCode: utils.GetStr("Código Empenho", rowIdx, &df),
		OperationType:  utils.GetStr("Tipo Operação", rowIdx, &df),
		OperationDate:  utils.GetStr("Data Operação", rowIdx, &df),
		Sequential:     utils.GetInt("Sequencial", rowIdx, &df),
		ItemQuantity:   utils.GetStr("Quantidade Item", rowIdx, &df),
		ItemUnitPrice:  utils.GetStr("Valor Unitário Item", rowIdx, &df),
		ItemTotalPrice: utils.GetStr("Valor Total Item", rowIdx, &df),
	}
}

func DfRowToPaymentImpactedCommitment(df dataframe.DataFrame, rowIdx int) types.PaymentImpactedCommitment {

	return types.PaymentImpactedCommitment{
		PaymentCode:                utils.GetStr("Código Pagamento", rowIdx, &df),
		CommitmentCode:             utils.GetStr("Código Empenho", rowIdx, &df),
		CompleteExpenseNature:      utils.GetStr("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                    utils.GetStr("Subitem", rowIdx, &df),
		PaidValueBRL:               utils.GetStr("Valor Pago (R$)", rowIdx, &df),
		RegisteredPayablesValueBRL: utils.GetStr("Valor Restos a Pagar Inscritos (R$)", rowIdx, &df),
		CanceledPayablesValueBRL:   utils.GetStr("Valor Restos a Pagar Cancelado (R$)", rowIdx, &df),
		OutstandingValuePaidBRL:    utils.GetStr("Valor Restos a Pagar Pagos (R$)", rowIdx, &df),
	}
}

func DfRowToLiquidationImpactedCommitment(df dataframe.DataFrame, rowIdx int) types.LiquidationImpactedCommitment {

	return types.LiquidationImpactedCommitment{
		LiquidationCode:               utils.GetStr("Código Liquidação", rowIdx, &df),
		CommitmentCode:                utils.GetStr("Código Empenho", rowIdx, &df),
		CompleteExpenseNature:         utils.GetStr("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                       utils.GetStr("Subitem", rowIdx, &df),
		LiquidatedValueBRL:            utils.GetStr("Valor Liquidado (R$)", rowIdx, &df),
		RegisteredPayablesValueBRL:    utils.GetStr("Valor Restos a Pagar Inscritos (R$)", rowIdx, &df),
		CanceledPayablesValueBRL:      utils.GetStr("Valor Restos a Pagar Cancelado (R$)", rowIdx, &df),
		OutstandingValueLiquidatedBRL: utils.GetStr("Valor Restos a Pagar Pagos (R$)", rowIdx, &df),
	}
}
