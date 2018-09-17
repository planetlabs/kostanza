package log

import (
	"go.uber.org/zap"
)

// Log is our global, configured logger.
var Log *zap.SugaredLogger

// Cfg is the logger configuration which is exposed for the sake of dynamic
// logging level reconfiguration.
var Cfg zap.Config

func init() {
	Cfg = zap.NewProductionConfig()
	logger, err := Cfg.Build()

	if err != nil {
		panic(err)
	}

	Log = logger.Sugar()
}
