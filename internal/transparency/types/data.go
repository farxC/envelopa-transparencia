package types

import (
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/go-gota/gota/dataframe"
)

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

type UnitCommitments struct {
	UgCode       string              `json:"ug_code"`
	UgName       string              `json:"ug_name"`
	Commitments  []store.Commitment  `json:"commitments"`
	Liquidations []store.Liquidation `json:"liquidations"`
	Payments     []store.Payment     `json:"payments"`
}

type CommitmentPayload struct {
	ExtractionDate  string            `json:"extraction_date"`
	UnitCommitments []UnitCommitments `json:"units"`
}
