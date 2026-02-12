package converter

import (
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/farxc/envelopa-transparencia/internal/transparency/utils"
	"github.com/go-gota/gota/dataframe"
)

func DfRowToCommitment(df dataframe.DataFrame, rowIdx int) store.Commitment {
	return store.Commitment{
		ID:                            utils.ParseInt64(utils.GetStr("Id Empenho", rowIdx, &df)),
		CommitmentCode:                utils.GetStr("Código Empenho", rowIdx, &df),
		ResumedCommitmentCode:         utils.GetStr("Código Empenho Resumido", rowIdx, &df),
		EmissionDate:                  utils.ParseDate(utils.GetStr("Data Emissão", rowIdx, &df)),
		Type:                          utils.GetStr("Tipo Empenho", rowIdx, &df),
		Process:                       utils.GetStr("Processo", rowIdx, &df),
		DocumentCodeType:              utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:                  utils.GetStr("Tipo Documento", rowIdx, &df),
		ManagementUnitName:            utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:            utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:                utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:                utils.GetStr("Gestão", rowIdx, &df),
		FavoredCode:                   utils.GetStr("Código Favorecido", rowIdx, &df),
		FavoredName:                   utils.GetStr("Favorecido", rowIdx, &df),
		ExpenseCategoryCode:           utils.GetInt16("Código Categoria de Despesa", rowIdx, &df),
		ExpenseCategory:               utils.GetStr("Categoria de Despesa", rowIdx, &df),
		ExpenseGroupCode:              utils.GetInt16("Código Grupo de Despesa", rowIdx, &df),
		ExpenseGroup:                  utils.GetStr("Grupo de Despesa", rowIdx, &df),
		ApplicationModalityCode:       utils.GetInt16("Código Modalidade de Aplicação", rowIdx, &df),
		ApplicationModality:           utils.GetStr("Modalidade de Aplicação", rowIdx, &df),
		ExpenseElementCode:            utils.GetInt16("Código Elemento de Despesa", rowIdx, &df),
		ExpenseElement:                utils.GetStr("Elemento de Despesa", rowIdx, &df),
		BudgetPlan:                    utils.GetStr("Plano Orçamentário", rowIdx, &df),
		BudgetPlanCode:                utils.GetInt32("Código Plano Orçamentário", rowIdx, &df),
		Observation:                   utils.GetStr("Observação", rowIdx, &df),
		CommitmentOriginalValue:       utils.ParseFloat(utils.GetStr("Valor Original do Empenho", rowIdx, &df)),
		CommitmentValueConvertedToBrl: utils.ParseFloat(utils.GetStr("Valor do Empenho Convertido pra R$", rowIdx, &df)),
		ConversionValueUsed:           utils.ParseFloat(utils.GetStr("Valor Utilizado na Conversão", rowIdx, &df)),
		Items:                         []store.CommitmentItem{},
	}
}

func DfRowToLiquidation(df dataframe.DataFrame, rowIdx int) store.Liquidation {
	return store.Liquidation{
		LiquidationCode:         utils.GetStr("Código Liquidação", rowIdx, &df),
		LiquidationCodeResumed:  utils.GetStr("Código Liquidação Resumido", rowIdx, &df),
		LiquidationEmissionDate: utils.ParseDate(utils.GetStr("Data Emissão", rowIdx, &df)),
		DocumentCodeType:        utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:            utils.GetStr("Tipo Documento", rowIdx, &df),
		ManagementUnitName:      utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:      utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:          utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:          utils.GetStr("Gestão", rowIdx, &df),
		FavoredCode:             utils.GetStr("Código Favorecido", rowIdx, &df),
		FavoredName:             utils.GetStr("Favorecido", rowIdx, &df),
		ExpenseCategoryCode:     utils.GetInt16("Código Categoria de Despesa", rowIdx, &df),
		ExpenseCategory:         utils.GetStr("Categoria de Despesa", rowIdx, &df),
		ExpenseGroupCode:        utils.GetInt16("Código Grupo de Despesa", rowIdx, &df),
		ExpenseGroup:            utils.GetStr("Grupo de Despesa", rowIdx, &df),
		ApplicationModalityCode: utils.GetInt16("Código Modalidade de Aplicação", rowIdx, &df),
		ApplicationModality:     utils.GetStr("Modalidade de Aplicação", rowIdx, &df),
		ExpenseElementCode:      utils.GetInt16("Código Elemento de Despesa", rowIdx, &df),
		ExpenseElement:          utils.GetStr("Elemento de Despesa", rowIdx, &df),
		BudgetPlan:              utils.GetStr("Plano Orçamentário", rowIdx, &df),
		BudgetPlanCode:          utils.GetInt32("Código Plano Orçamentário", rowIdx, &df),
		Observation:             utils.GetStr("Observação", rowIdx, &df),
		ImpactedCommitments:     []store.LiquidationImpactedCommitment{},
	}
}

func DfRowToPayment(df dataframe.DataFrame, rowIdx int) store.Payment {
	return store.Payment{
		PaymentCode:             utils.GetStr("Código Pagamento", rowIdx, &df),
		PaymentCodeResumed:      utils.GetStr("Código Pagamento Resumido", rowIdx, &df),
		PaymentEmissionDate:     utils.ParseDate(utils.GetStr("Data Emissão", rowIdx, &df)),
		DocumentCodeType:        utils.GetStr("Código Tipo Documento", rowIdx, &df),
		DocumentType:            utils.GetStr("Tipo Documento", rowIdx, &df),
		FavoredCode:             utils.GetStr("Código Favorecido", rowIdx, &df),
		FavoredName:             utils.GetStr("Favorecido", rowIdx, &df),
		ManagementUnitName:      utils.GetStr("Unidade Gestora", rowIdx, &df),
		ManagementUnitCode:      utils.GetInt("Código Unidade Gestora", rowIdx, &df),
		ManagementCode:          utils.GetInt("Código Gestão", rowIdx, &df),
		ManagementName:          utils.GetStr("Gestão", rowIdx, &df),
		ExpenseCategoryCode:     utils.GetInt16("Código Categoria de Despesa", rowIdx, &df),
		ExpenseCategory:         utils.GetStr("Categoria de Despesa", rowIdx, &df),
		ExpenseGroupCode:        utils.GetInt16("Código Grupo de Despesa", rowIdx, &df),
		ExpenseGroup:            utils.GetStr("Grupo de Despesa", rowIdx, &df),
		ApplicationModalityCode: utils.GetInt16("Código Modalidade de Aplicação", rowIdx, &df),
		ApplicationModality:     utils.GetStr("Modalidade de Aplicação", rowIdx, &df),
		ExpenseElementCode:      utils.GetInt16("Código Elemento de Despesa", rowIdx, &df),
		ExpenseElement:          utils.GetStr("Elemento de Despesa", rowIdx, &df),
		BudgetPlan:              utils.GetStr("Plano Orçamentário", rowIdx, &df),
		BudgetPlanCode:          utils.GetInt32("Código Plano Orçamentário", rowIdx, &df),
		Observation:             utils.GetStr("Observação", rowIdx, &df),
		ExtraBudgetary:          utils.ParseBool(utils.GetStr("Extraorçamentário", rowIdx, &df)),
		Process:                 utils.GetStr("Processo", rowIdx, &df),
		OriginalPaymentValue:    utils.ParseFloat(utils.GetStr("Valor Original do Pagamento", rowIdx, &df)),
		ConvertedPaymentValue:   utils.ParseFloat(utils.GetStr("Valor do Pagamento Convertido pra R$", rowIdx, &df)),
		ConversionUsedValue:     utils.ParseFloat(utils.GetStr("Valor Utilizado na Conversão", rowIdx, &df)),
		ImpactedCommitments:     []store.PaymentImpactedCommitment{},
	}
}

func DfRowToCommitmentItem(df dataframe.DataFrame, rowIdx int) store.CommitmentItem {
	return store.CommitmentItem{
		CommitmentID:            utils.ParseInt64(utils.GetStr("Id Empenho", rowIdx, &df)),
		CommitmentCode:          utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseCategoryCode:     utils.GetInt16("Código Categoria de Despesa", rowIdx, &df),
		ExpenseCategory:         utils.GetStr("Categoria de Despesa", rowIdx, &df),
		ExpenseGroupCode:        utils.GetInt16("Código Grupo de Despesa", rowIdx, &df),
		ExpenseGroup:            utils.GetStr("Grupo de Despesa", rowIdx, &df),
		ApplicationModalityCode: utils.GetInt16("Código Modalidade de Aplicação", rowIdx, &df),
		ApplicationModality:     utils.GetStr("Modalidade de Aplicação", rowIdx, &df),
		ExpenseElementCode:      utils.GetInt16("Código Elemento de Despesa", rowIdx, &df),
		ExpenseElement:          utils.GetStr("Elemento de Despesa", rowIdx, &df),
		SubExpenseElement:       utils.GetStr("SubElemento de Despesa", rowIdx, &df),
		SubExpenseElementCode:   utils.GetInt16("Código SubElemento de Despesa", rowIdx, &df),
		Description:             utils.GetStr("Descrição", rowIdx, &df),
		Sequential:              utils.ParseInt16(utils.GetInt("Sequencial", rowIdx, &df)),
		Quantity:                utils.ParseFloat(utils.GetStr("Quantidade", rowIdx, &df)),
		UnitPrice:               utils.ParseFloat(utils.GetStr("Valor Unitário", rowIdx, &df)),
		CurrentValue:            utils.ParseFloat(utils.GetStr("Valor Atual", rowIdx, &df)),
		TotalPrice:              utils.ParseFloat(utils.GetStr("Valor Total", rowIdx, &df)),
		History:                 []store.CommitmentItemsHistory{},
	}
}

func DfRowToCommitmentItemHistory(df dataframe.DataFrame, rowIdx int) store.CommitmentItemsHistory {
	return store.CommitmentItemsHistory{
		CommitmentID:   utils.ParseInt64(utils.GetStr("Id Empenho", rowIdx, &df)),
		CommitmentCode: utils.GetStr("Código Empenho", rowIdx, &df),
		OperationType:  utils.GetStr("Tipo Operação", rowIdx, &df),
		OperationDate:  utils.ParseDate(utils.GetStr("Data Operação", rowIdx, &df)),
		Sequential:     utils.ParseInt16(utils.GetInt("Sequencial", rowIdx, &df)),
		ItemQuantity:   utils.ParseFloat(utils.GetStr("Quantidade Item", rowIdx, &df)),
		ItemUnitPrice:  utils.ParseFloat(utils.GetStr("Valor Unitário Item", rowIdx, &df)),
		ItemTotalPrice: utils.ParseFloat(utils.GetStr("Valor Total Item", rowIdx, &df)),
	}
}

func DfRowToPaymentImpactedCommitment(df dataframe.DataFrame, rowIdx int) store.PaymentImpactedCommitment {
	return store.PaymentImpactedCommitment{
		PaymentCode:                utils.GetStr("Código Pagamento", rowIdx, &df),
		CommitmentCode:             utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseNatureCodeComplete:  utils.GetInt64("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                    utils.GetStr("Subitem", rowIdx, &df),
		PaidValueBRL:               utils.ParseFloat(utils.GetStr("Valor Pago (R$)", rowIdx, &df)),
		RegisteredPayablesValueBRL: utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Inscritos (R$)", rowIdx, &df)),
		CanceledPayablesValueBRL:   utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Cancelado (R$)", rowIdx, &df)),
		OutstandingValuePaidBRL:    utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Pagos (R$)", rowIdx, &df)),
	}
}

func DfRowToLiquidationImpactedCommitment(df dataframe.DataFrame, rowIdx int) store.LiquidationImpactedCommitment {
	return store.LiquidationImpactedCommitment{
		LiquidationCode:               utils.GetStr("Código Liquidação", rowIdx, &df),
		CommitmentCode:                utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseNatureCodeComplete:     utils.GetInt64("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                       utils.GetStr("Subitem", rowIdx, &df),
		LiquidatedValueBRL:            utils.ParseFloat(utils.GetStr("Valor Liquidado (R$)", rowIdx, &df)),
		RegisteredPayablesValueBRL:    utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Inscritos (R$)", rowIdx, &df)),
		CanceledPayablesValueBRL:      utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Cancelado (R$)", rowIdx, &df)),
		OutstandingValueLiquidatedBRL: utils.ParseFloat(utils.GetStr("Valor Restos a Pagar Pagos (R$)", rowIdx, &df)),
	}
}
