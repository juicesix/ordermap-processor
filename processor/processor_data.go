package processor

import (
	"context"
	"time"

	"github.com/jinzhu/gorm"
)

type ProcessedData struct {
	Ctx *context.Context
	// 业务参数
	BusinessData interface{}

	DB *gorm.DB

	Err error

	CurrentTime time.Time

	LogHead string
}

func NewProcessedData(ctx context.Context, BusinessData interface{}) *ProcessedData {
	return &ProcessedData{
		Ctx:          &ctx,
		BusinessData: BusinessData,
	}
}
