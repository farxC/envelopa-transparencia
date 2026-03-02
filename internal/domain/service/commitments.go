package service

import (
	"encoding/json"
	"time"
)

type GetCommitmentInformationFilter struct {
	CommitmentCodes     []string
	ManagementUnitCodes []string
	ManagementCode      string
	StartDate           time.Time
	EndDate             time.Time
}

type CommitmentItemInformation struct {
	ItemDescription   string  `db:"item_description" json:"item_description"`
	CurrentValue      float64 `db:"current_value" json:"current_value"`
	SubExpenseElement string  `db:"sub_expense_element" json:"sub_expense_element"`
	Description       string  `db:"description" json:"description"`
	Quantity          float64 `db:"quantity" json:"quantity"`
	Sequential        int     `db:"sequential" json:"sequential"`
}

type CommitmentInformation struct {
	ManagementUnitCode     string                      `db:"management_unit_code" json:"management_unit_code"`
	CommitmentCode         string                      `db:"commitment_code" json:"commitment_code"`
	CommitmentTotalValue   float64                     `db:"commitment_total_value" json:"commitment_total_value"`
	CommitmentEmissionDate time.Time                   `db:"commitment_emission_date" json:"commitment_emission_date"`
	CommitmentProcess      string                      `db:"commitment_process" json:"commitment_process"`
	CommitmentType         string                      `db:"commitment_type" json:"commitment_type"`
	CommitmentFavored      string                      `db:"commitment_favored" json:"commitment_favored"`
	CommitmentFavoredCode  string                      `db:"commitment_favored_code" json:"commitment_favored_code"`
	CommitmentItemsRaw     json.RawMessage             `db:"commitment_items" json:"-"`
	CommitmentItems        []CommitmentItemInformation `json:"commitment_items"`
}
