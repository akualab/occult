package main

import "github.com/akualab/coap"

type Options struct {
	trainData interface{}
	testData  interface{}
}

func movieTrainFunc(idx uint64, ctx *coap.Context, in ...coap.Processor) (coap.Value, error) {
	opt := ctx.Options.(*Options)
	_ = opt
	return nil, nil
}

func createApp() {

	opt := &Options{}
	app := coap.NewApp("test")
	train := app.AddSource(movieTrainFunc, opt, nil)
	_ = train
}
