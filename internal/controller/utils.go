package controller

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/go-xmlfmt/xmlfmt"
	"strings"
	"time"
)

func int32Ptr(i int32) *int32 { return &i }

func map2String(kv map[string]string) string {
	var sb strings.Builder
	for key, value := range kv {
		sb.WriteString(key)
		sb.WriteString("=")
		sb.WriteString(value)
		sb.WriteString("\n")
	}
	return sb.String()
}

func map2Xml(properties map[string]string) string {
	var res string = `<configuration>`
	for key, value := range properties {
		property := `<property>
	<name>` + key + `</name>
	<value>` + value + `</value>
</property>`
		res = res + property
	}

	res = res + `</configuration>`
	res = xmlfmt.FormatXML(res, "", "  ")

	return res
}

func LogInfoInterval(ctx context.Context, interval int, msg string) {
	logger, _ := logr.FromContext(ctx)
	if interval < 5 {
		interval = 5
	}
	if time.Now().Second()%interval == 0 {
		logger.Info(msg)
	}
}
