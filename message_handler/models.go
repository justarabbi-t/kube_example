package message_handler

import (
	"maps"
	"strings"
	"sync"
)

type SafeAppSlice struct {
	mu      sync.Mutex
	appList []AnApp
}

type HandlerChannels[T any] struct {
	errChan      chan error
	addChan      chan T
	updChan      chan T
	delChan      chan T
	producerChan chan any
	consumerChan chan any
}

type KubeLike interface {
	GetName() string
	GetNamespace() string
	GetLabels() map[string]string
}

func NewSafeAppSlice() *SafeAppSlice {
	return &SafeAppSlice{
		mu:      sync.Mutex{},
		appList: []AnApp{},
	}
}

func checkMap(k string, m map[string]string) bool {
	_, ok := m[k]
	return ok
}

type AnApp struct {
	Name string
	Tags map[string]string
}

func NewApp(name string, tags map[string]string) AnApp {
	return AnApp{name, tags}
}

func (a1 AnApp) tagsEqual(a2 AnApp) bool {
	return maps.Equal(a1.Tags, a2.Tags)
}
func (a1 AnApp) DeepEqual(a2 AnApp) bool {
	return a1.Name == a2.Name && a1.getTenantName() == a2.getTenantName() && a1.tagsEqual(a2)
}

func (a1 AnApp) Equal(a2 AnApp) bool {
	return a1.Name == a2.Name && a1.getTenantName() == a2.getTenantName()
}

func (a AnApp) getVrf() string {
	if checkMap("vrf", a.Tags) {
		return a.Tags["vrf"]
	}
	return ""
}

func (a AnApp) isExportBgpTenant() bool {
	if checkMap("vrf", a.Tags) && checkMap("exportBgp", a.Tags) && checkMap("tenantName", a.Tags) {
		return true
	}
	return false
}

func (a AnApp) getAdvertiseTypes() []string {
	advertTypes := []string{}

	if checkMap("advertTypes", a.Tags) {
		advertTypes = append(advertTypes, strings.Split(a.Tags["advertTypes"], ".")...)
	}
	return advertTypes
}

func (a AnApp) getTenantName() string {
	if checkMap("tenantName", a.Tags) {
		return a.Tags["tenantName"]
	}
	return ""
}

type matchExpression struct {
	key   string
	value string
}

func (m1 matchExpression) Equal(m2 matchExpression) bool {
	return m1.key == m2.key && m1.value == m2.value
}

func (m *matchExpression) asMap() map[string]string {
	return map[string]string{
		m.key: m.value,
	}
}
