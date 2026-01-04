package transparency

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

type CommitmentItemHistory struct {
	OperationType  string  `json:"operation_type"`
	OperationDate  string  `json:"operation_date"`
	ItemQuantity   int     `json:"item_quantity"`
	ItemUnitPrice  float32 `json:"item_unit_price"`
	ItemTotalPrice float32 `json:"item_total_price"`
}
type CommitmentItem struct {
	ExpenseCategory    string                  `json:"expense_category"`
	ExpenseGroup       string                  `json:"expense_group"`
	AplicationModality string                  `json:"aplication_modality"`
	ExpenseElement     string                  `json:"expense_element"`
	Description        string                  `json:"description"`
	Quantity           int                     `json:"quantity"`
	Sequential         int                     `json:"sequential"`
	UnitPrice          float32                 `json:"unit_price"`
	CurrentValue       float32                 `json:"current_value"`
	TotalPrice         float32                 `json:"total_price"`
	History            []CommitmentItemHistory `json:"history"`
}

type Commitment struct {
	CommitmentCode                string           `json:"commitment_code"`
	ResumedCommitmentCode         string           `json:"resumed_commitment_code"`
	EmitionDate                   string           `json:"emition_date"`
	Type                          string           `json:"type"`
	ManagementCode                string           `json:"management_code"`
	ManagementName                string           `json:"management_name"`
	FavoredCode                   string           `json:"favored_code"`
	ExpenseNature                 string           `json:"expense_nature"`
	CompleteExpenseNature         string           `json:"complete_expense_nature"`
	BudgetPlan                    string           `json:"budget_plan"`
	CommitmentOriginalValue       float32          `json:"commitment_original_value"`
	CommitmentValueConvertedToBRL float32          `json:"commitment_value_converted_to_brl"`
	ConversionValueUsed           float32          `json:"conversion_value_used"`
	Items                         []CommitmentItem `json:"items"`
}

type Liquidation struct {
	LiquidationCode        string `json:"liquidation_code"`
	LiquidationCodeResumed string `json:"liquidation_code_resumed"`
	LiquidationEmitionDate string `json:"liquidation_emition_date"`
	DocumentCodeType       string `json:"document_code_type"`
	DocumentType           string `json:"document_type"`
	FavoredCode            string `json:"favored_code"`
	FavoredName            string `json:"favored_name"`
	Observation            string `json:"observation"`
}

type Payment struct {
	PaymentCode           string `json:"payment_code"`
	PaymentCodeResumed    string `json:"payment_code_resumed"`
	PaymentEmitionDate    string `json:"payment_emition_date"`
	DocumentCodeType      string `json:"document_code_type"`
	DocumentType          string `json:"document_type"`
	FavoredCode           string `json:"favored_code"`
	FavoredName           string `json:"favored_name"`
	Observation           string `json:"observation"`
	ExtraBudgetary        string `json:"extra_budgetary"`
	Process               string `json:"process"`
	OriginalPaymentValue  string `json:"original_payment_value"`
	ConvertedPaymentValue string `json:"converted_payment_value"`
	ConversionUsedValue   string `json:"conversion_used_value"`
}
type UnitCommitments struct {
	UgCode       string        `json:"ug_code"`
	UgName       string        `json:"ug_name"`
	Commitments  []Commitment  `json:"commitments"`
	Liquidations []Liquidation `json:"liquidations"`
	Payments     []Payment     `json:"payments"`
}

type CommitmentPayload struct {
	UnitCommitments []UnitCommitments `json:"units"`
	ExtractionDate  string            `json:"extraction_date"`
}
