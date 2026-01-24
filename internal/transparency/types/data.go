package types

import "github.com/go-gota/gota/dataframe"

type DataType int

const (
	DespesasEmpenho DataType = iota
	DespesasItemEmpenho
	DespesasItemEmpenhoHistorico
	DespesasLiquidacao
	DespesasPagamento
	DespesasPagamentoListaBancos
	DespesasPagamentoEmpenhosImpactados
	DespesasPagamentoFavoricidosFinais
	DespesasPagamentoListaFaturas
	DespesasPagamentoListaPrecatorios
	DespesasLiquidacaoEmpenhos
	DespesasLiquidacaoEmpenhosImpactados
)

const (
	DespesasEmpenhoDataType                      = "_Despesas_Empenho.csv"
	DespesasItemEmpenhoDataType                  = "_Despesas_ItemEmpenho.csv"
	DespesasItemEmpenhoHistoricoDataType         = "_Despesas_ItemEmpenhoHistorico.csv"
	DespesasLiquidacaoDataType                   = "_Despesas_Liquidacao.csv"
	DespesasPagamentoDataType                    = "_Despesas_Pagamento.csv"
	DespesasLiquidacaoEmpenhosImpactadosDataType = "_Despesas_Liquidacao_EmpenhosImpactados.csv"
	DespesasPagamentoEmpenhosImpactadosDataType  = "_Despesas_Pagamento_EmpenhosImpactados.csv"
	DespesasPagamentoListaBancosDataType         = "_Despesas_Pagamento_ListaBancos.csv"
	DespesasPagamentoListaFaturasDataType        = "_Despesas_Pagamento_ListaFaturas.csv"
	DespesasPagamentoListaPrecatoriosDataType    = "_Despesas_Pagamento_ListaPrecatorios.csv"
)

var DataTypeNames = map[DataType]string{
	DespesasEmpenho:                      "Despesas Empenho",
	DespesasItemEmpenho:                  "Despesas Item Empenho",
	DespesasItemEmpenhoHistorico:         "Despesas Item Empenho Histórico",
	DespesasLiquidacao:                   "Despesas Liquidação",
	DespesasPagamento:                    "Despesas Pagamento",
	DespesasLiquidacaoEmpenhosImpactados: "Despesas Liquidação Empenhos Impactados",
	DespesasPagamentoEmpenhosImpactados:  "Despesas Pagamento Empenhos Impactados",
	DespesasPagamentoListaBancos:         "Despesas Pagamento Lista Bancos",
	DespesasPagamentoListaFaturas:        "Despesas Pagamento Lista Faturas",
	DespesasPagamentoListaPrecatorios:    "Despesas Pagamento Lista Precatórios",
}

type MatchingDataframe struct {
	Dataframe dataframe.DataFrame
	Type      DataType
}

type CommitmentItems struct {
	CommitmentCode string
	ItemsDf        dataframe.DataFrame
}

type OutputExtractionFiles struct {
	Date  string
	Files map[DataType]string
}

type CommitmentItemHistory struct {
	CommitmentId   string `json:"commitment_id"`
	CommitmentCode string `json:"commitment_code"`
	OperationType  string `json:"operation_type"`
	OperationDate  string `json:"operation_date"`
	ItemQuantity   string `json:"item_quantity"`
	ItemUnitPrice  string `json:"item_unit_price"`
	ItemTotalPrice string `json:"item_total_price"`
	Sequential     int    `json:"sequential"`
}
type CommitmentItem struct {
	CommitmentId       string                  `json:"commitment_id"`
	CommitmentCode     string                  `json:"commitment_code"`
	ExpenseCategory    string                  `json:"expense_category"`
	ExpenseGroup       string                  `json:"expense_group"`
	AplicationModality string                  `json:"aplication_modality"`
	ExpenseElement     string                  `json:"expense_element"`
	Description        string                  `json:"description"`
	Quantity           string                  `json:"quantity"`
	Sequential         int                     `json:"sequential"`
	UnitPrice          string                  `json:"unit_price"`
	CurrentValue       string                  `json:"current_value"`
	TotalPrice         string                  `json:"total_price"`
	History            []CommitmentItemHistory `json:"history"`
}

type Commitment struct {
	CommitmentId                  string           `json:"commitment_id"`
	CommitmentCode                string           `json:"commitment_code"`
	ResumedCommitmentCode         string           `json:"resumed_commitment_code"`
	EmitionDate                   string           `json:"emition_date"`
	Type                          string           `json:"type"`
	Process                       string           `json:"process"`
	ManagementUnitName            string           `json:"management_unit_name"`
	ManagementUnitCode            int              `json:"management_unit_code"`
	ManagementCode                int              `json:"management_code"`
	ManagementName                string           `json:"management_name"`
	FavoredName                   string           `json:"favored_name"`
	FavoredCode                   string           `json:"favored_code"`
	ExpenseNature                 string           `json:"expense_nature"`
	CompleteExpenseNature         string           `json:"complete_expense_nature"`
	BudgetPlan                    string           `json:"budget_plan"`
	CommitmentOriginalValue       string           `json:"commitment_original_value"`
	CommitmentValueConvertedToBRL string           `json:"commitment_value_converted_to_brl"`
	ConversionValueUsed           string           `json:"conversion_value_used"`
	Items                         []CommitmentItem `json:"items"`
}

type LiquidationImpactedCommitment struct {
	LiquidationCode               string `json:"liquidation_code"`
	CommitmentCode                string `json:"commitment_code"`
	CompleteExpenseNature         string `json:"complete_expense_nature"`
	Subitem                       string `json:"subitem"`
	LiquidatedValueBRL            string `json:"liquidated_value_brl"`
	RegisteredPayablesValueBRL    string `json:"registered_payables_value_brl"`
	CanceledPayablesValueBRL      string `json:"canceled_payables_value_brl"`
	OutstandingValueLiquidatedBRL string `json:"outstanding_value_liquidated_brl"`
}

type Liquidation struct {
	LiquidationCode        string                          `json:"liquidation_code"`
	LiquidationCodeResumed string                          `json:"liquidation_code_resumed"`
	LiquidationEmitionDate string                          `json:"liquidation_emition_date"`
	ManagementUnitName     string                          `json:"management_unit_name"`
	ManagementUnitCode     int                             `json:"management_unit_code"`
	ManagementCode         int                             `json:"management_code"`
	ManagementName         string                          `json:"management_name"`
	DocumentCodeType       string                          `json:"document_code_type"`
	DocumentType           string                          `json:"document_type"`
	FavoredCode            string                          `json:"favored_code"`
	FavoredName            string                          `json:"favored_name"`
	Observation            string                          `json:"observation"`
	ImpactedCommitments    []LiquidationImpactedCommitment `json:"impacted_commitments"`
}

type PaymentImpactedCommitment struct {
	PaymentCode                string `json:"payment_code"`
	CommitmentCode             string `json:"commitment_code"`
	CompleteExpenseNature      string `json:"complete_expense_nature"`
	Subitem                    string `json:"subitem"`
	PaidValueBRL               string `json:"paid_value_brl"`
	RegisteredPayablesValueBRL string `json:"registered_payables_value_brl"`
	CanceledPayablesValueBRL   string `json:"canceled_payables_value_brl"`
	OutstandingValuePaidBRL    string `json:"outstanding_value_paid_brl"`
}

type Payment struct {
	PaymentCode           string                      `json:"payment_code"`
	PaymentCodeResumed    string                      `json:"payment_code_resumed"`
	PaymentEmitionDate    string                      `json:"payment_emition_date"`
	DocumentCodeType      string                      `json:"document_code_type"`
	ManagementUnitName    string                      `json:"management_unit_name"`
	ManagementUnitCode    int                         `json:"management_unit_code"`
	ManagementCode        int                         `json:"management_code"`
	ManagementName        string                      `json:"management_name"`
	DocumentType          string                      `json:"document_type"`
	FavoredCode           string                      `json:"favored_code"`
	FavoredName           string                      `json:"favored_name"`
	Observation           string                      `json:"observation"`
	ExtraBudgetary        string                      `json:"extra_budgetary"`
	Process               string                      `json:"process"`
	OriginalPaymentValue  string                      `json:"original_payment_value"`
	ConvertedPaymentValue string                      `json:"converted_payment_value"`
	ConversionUsedValue   string                      `json:"conversion_used_value"`
	ImpactedCommitments   []PaymentImpactedCommitment `json:"impacted_commitments"`
}

type UnitCommitments struct {
	UgCode       string        `json:"ug_code"`
	UgName       string        `json:"ug_name"`
	Commitments  []Commitment  `json:"commitments"`
	Liquidations []Liquidation `json:"liquidations"`
	Payments     []Payment     `json:"payments"`
}

type CommitmentPayload struct {
	ExtractionDate  string            `json:"extraction_date"`
	UnitCommitments []UnitCommitments `json:"units"`
}
