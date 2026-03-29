package portal

import (
	"fmt"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/utils"
	"github.com/go-gota/gota/dataframe"
)

func parseFloatField(df dataframe.DataFrame, rowIdx int, column string) (float64, error) {
	value, err := utils.ParseFloat(utils.GetStr(column, rowIdx, &df))
	if err != nil {
		return 0, fmt.Errorf("row=%d column=%q: %w", rowIdx, column, err)
	}
	return value, nil
}

func DfRowToCommitment(df dataframe.DataFrame, rowIdx int) (model.Commitment, error) {
	originalValue, err := parseFloatField(df, rowIdx, "Valor Original do Empenho")
	if err != nil {
		return model.Commitment{}, err
	}
	convertedValue, err := parseFloatField(df, rowIdx, "Valor do Empenho Convertido pra R$")
	if err != nil {
		return model.Commitment{}, err
	}
	conversionValue, err := parseFloatField(df, rowIdx, "Valor Utilizado na Conversão")
	if err != nil {
		return model.Commitment{}, err
	}

	return model.Commitment{
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
		CommitmentOriginalValue:       originalValue,
		CommitmentValueConvertedToBrl: convertedValue,
		ConversionValueUsed:           conversionValue,
		Items:                         []model.CommitmentItem{},
	}, nil
}

func DfRowToLiquidation(df dataframe.DataFrame, rowIdx int) model.Liquidation {
	return model.Liquidation{
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
		ImpactedCommitments:     []model.LiquidationImpactedCommitment{},
	}
}

func DfRowToPayment(df dataframe.DataFrame, rowIdx int) (model.Payment, error) {
	originalValue, err := parseFloatField(df, rowIdx, "Valor Original do Pagamento")
	if err != nil {
		return model.Payment{}, err
	}
	convertedValue, err := parseFloatField(df, rowIdx, "Valor do Pagamento Convertido pra R$")
	if err != nil {
		return model.Payment{}, err
	}
	conversionValue, err := parseFloatField(df, rowIdx, "Valor Utilizado na Conversão")
	if err != nil {
		return model.Payment{}, err
	}

	return model.Payment{
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
		OriginalPaymentValue:    originalValue,
		ConvertedPaymentValue:   convertedValue,
		ConversionUsedValue:     conversionValue,
		ImpactedCommitments:     []model.PaymentImpactedCommitment{},
	}, nil
}

func DfRowToCommitmentItem(df dataframe.DataFrame, rowIdx int) (model.CommitmentItem, error) {
	quantity, err := parseFloatField(df, rowIdx, "Quantidade")
	if err != nil {
		return model.CommitmentItem{}, err
	}
	unitPrice, err := parseFloatField(df, rowIdx, "Valor Unitário")
	if err != nil {
		return model.CommitmentItem{}, err
	}
	currentValue, err := parseFloatField(df, rowIdx, "Valor Atual")
	if err != nil {
		return model.CommitmentItem{}, err
	}
	totalPrice, err := parseFloatField(df, rowIdx, "Valor Total")
	if err != nil {
		return model.CommitmentItem{}, err
	}

	return model.CommitmentItem{
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
		Quantity:                quantity,
		UnitPrice:               unitPrice,
		CurrentValue:            currentValue,
		TotalPrice:              totalPrice,
		History:                 []model.CommitmentItemsHistory{},
	}, nil
}

func DfRowToCommitmentItemHistory(df dataframe.DataFrame, rowIdx int) (model.CommitmentItemsHistory, error) {
	itemQuantity, err := parseFloatField(df, rowIdx, "Quantidade Item")
	if err != nil {
		return model.CommitmentItemsHistory{}, err
	}
	itemUnitPrice, err := parseFloatField(df, rowIdx, "Valor Unitário Item")
	if err != nil {
		return model.CommitmentItemsHistory{}, err
	}
	itemTotalPrice, err := parseFloatField(df, rowIdx, "Valor Total Item")
	if err != nil {
		return model.CommitmentItemsHistory{}, err
	}

	return model.CommitmentItemsHistory{
		CommitmentID:   utils.ParseInt64(utils.GetStr("Id Empenho", rowIdx, &df)),
		CommitmentCode: utils.GetStr("Código Empenho", rowIdx, &df),
		OperationType:  utils.GetStr("Tipo Operação", rowIdx, &df),
		OperationDate:  utils.ParseDate(utils.GetStr("Data Operação", rowIdx, &df)),
		Sequential:     utils.ParseInt16(utils.GetInt("Sequencial", rowIdx, &df)),
		ItemQuantity:   itemQuantity,
		ItemUnitPrice:  itemUnitPrice,
		ItemTotalPrice: itemTotalPrice,
	}, nil
}

func DfRowToPaymentImpactedCommitment(df dataframe.DataFrame, rowIdx int) (model.PaymentImpactedCommitment, error) {
	paidValue, err := parseFloatField(df, rowIdx, "Valor Pago (R$)")
	if err != nil {
		return model.PaymentImpactedCommitment{}, err
	}
	registeredPayablesValue, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Inscritos (R$)")
	if err != nil {
		return model.PaymentImpactedCommitment{}, err
	}
	canceledPayablesValue, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Cancelado (R$)")
	if err != nil {
		return model.PaymentImpactedCommitment{}, err
	}
	outstandingValuePaid, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Pagos (R$)")
	if err != nil {
		return model.PaymentImpactedCommitment{}, err
	}

	return model.PaymentImpactedCommitment{
		PaymentCode:                utils.GetStr("Código Pagamento", rowIdx, &df),
		CommitmentCode:             utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseNatureCodeComplete:  utils.GetInt64("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                    utils.GetStr("Subitem", rowIdx, &df),
		PaidValueBRL:               paidValue,
		RegisteredPayablesValueBRL: registeredPayablesValue,
		CanceledPayablesValueBRL:   canceledPayablesValue,
		OutstandingValuePaidBRL:    outstandingValuePaid,
	}, nil
}

func DfRowToLiquidationImpactedCommitment(df dataframe.DataFrame, rowIdx int) (model.LiquidationImpactedCommitment, error) {
	liquidatedValue, err := parseFloatField(df, rowIdx, "Valor Liquidado (R$)")
	if err != nil {
		return model.LiquidationImpactedCommitment{}, err
	}
	registeredPayablesValue, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Inscritos (R$)")
	if err != nil {
		return model.LiquidationImpactedCommitment{}, err
	}
	canceledPayablesValue, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Cancelado (R$)")
	if err != nil {
		return model.LiquidationImpactedCommitment{}, err
	}
	outstandingValueLiquidated, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Pagos (R$)")
	if err != nil {
		return model.LiquidationImpactedCommitment{}, err
	}

	return model.LiquidationImpactedCommitment{
		LiquidationCode:               utils.GetStr("Código Liquidação", rowIdx, &df),
		CommitmentCode:                utils.GetStr("Código Empenho", rowIdx, &df),
		ExpenseNatureCodeComplete:     utils.GetInt64("Código Natureza Despesa Completa", rowIdx, &df),
		Subitem:                       utils.GetStr("Subitem", rowIdx, &df),
		LiquidatedValueBRL:            liquidatedValue,
		RegisteredPayablesValueBRL:    registeredPayablesValue,
		CanceledPayablesValueBRL:      canceledPayablesValue,
		OutstandingValueLiquidatedBRL: outstandingValueLiquidated,
	}, nil
}

func DfRowToExpenseExecution(df dataframe.DataFrame, rowIdx int) (model.ExpenseExecution, error) {
	committedValue, err := parseFloatField(df, rowIdx, "Valor Empenhado (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}
	liquidatedValue, err := parseFloatField(df, rowIdx, "Valor Liquidado (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}
	paidValue, err := parseFloatField(df, rowIdx, "Valor Pago (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}
	registeredPayables, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Inscritos (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}
	cancelledPayables, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Cancelado (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}
	paidPayables, err := parseFloatField(df, rowIdx, "Valor Restos a Pagar Pagos (R$)")
	if err != nil {
		return model.ExpenseExecution{}, err
	}

	return model.ExpenseExecution{
		YearAndMonth:                utils.GetStr("Ano e mês do lançamento", rowIdx, &df),
		SuperiorOrganCode:           utils.GetInt32("Código Órgão Superior", rowIdx, &df),
		SuperiorOrganName:           utils.GetStr("Nome Órgão Superior", rowIdx, &df),
		SubordinatedOrganCode:       utils.GetInt32("Código Órgão Subordinado", rowIdx, &df),
		SubordinatedOrganName:       utils.GetStr("Nome Órgão Subordinado", rowIdx, &df),
		ManagementUnitCode:          utils.GetInt32("Código Unidade Gestora", rowIdx, &df),
		ManagementUnitName:          utils.GetStr("Nome Unidade Gestora", rowIdx, &df),
		ManagementCode:              utils.GetInt32("Código Gestão", rowIdx, &df),
		ManagementName:              utils.GetStr("Nome Gestão", rowIdx, &df),
		ActionCode:                  utils.GetStr("Código Ação", rowIdx, &df),
		ActionName:                  utils.GetStr("Nome Ação", rowIdx, &df),
		BudgetPlanCode:              utils.GetInt32("Código Plano Orçamentário", rowIdx, &df),
		BudgetPlanName:              utils.GetStr("Plano Orçamentário", rowIdx, &df),
		FederativeUnit:              utils.GetStr("UF", rowIdx, &df),
		Municipality:                utils.GetStr("Município", rowIdx, &df),
		AuthorAmendamentCode:        utils.GetInt32("Código Autor Emenda", rowIdx, &df),
		AuthorAmendamentName:        utils.GetStr("Nome Autor Emenda", rowIdx, &df),
		EconomicCategoryCode:        utils.GetInt32("Código Categoria Econômica", rowIdx, &df),
		EconomicCategoryName:        utils.GetStr("Nome Categoria Econômica", rowIdx, &df),
		ExpenseGroupCode:            utils.GetInt32("Código Grupo de Despesa", rowIdx, &df),
		ExpenseGroupName:            utils.GetStr("Nome Grupo de Despesa", rowIdx, &df),
		ExpenseCategoryCode:         utils.GetInt32("Código Elemento de Despesa", rowIdx, &df),
		ExpenseCategoryName:         utils.GetStr("Nome Elemento de Despesa", rowIdx, &df),
		ExpenseModalityCode:         utils.GetInt32("Código Modalidade da Despesa", rowIdx, &df),
		ExpenseModalityName:         utils.GetStr("Modalidade da Despesa", rowIdx, &df),
		CommittedValueBRL:           committedValue,
		LiquidatedValueBRL:          liquidatedValue,
		PaidValueBRL:                paidValue,
		RegisteredPayablesAmountBRL: registeredPayables,
		CancelledPayablesAmountBRL:  cancelledPayables,
		PaidPayablesAmountBRL:       paidPayables,
	}, nil
}
