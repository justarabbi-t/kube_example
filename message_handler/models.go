package message_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/justarabbi-t/kube_example.git/models"
	appsv1 "k8s.io/api/apps/v1"
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

var AddDplBaseMessage = map[string]Message{
	"Add": {
		Action:        Add,
		ActionSubject: Deployment,
		Topic:         AppList,
	},
	"Del": {
		Action:        Del,
		ActionSubject: Deployment,
		Topic:         AppList,
	},
	"Upd": {
		Action:        Upd,
		ActionSubject: Deployment,
		Topic:         AppList,
	},
}

type MessageType int

const (
	DeployMsg MessageType = iota
)

var msgTypeMap = map[MessageType]MessageLike{
	DeployMsg: NewEmptyDeploymentMessage(),
}

var msgTypeStructMap = map[MessageLike]MessageType{
	&DeploymentMessage{}: DeployMsg,
}

var msgTypeStringMap = map[MessageType]string{
	DeployMsg: "DeploymentMessage",
}

func (m MessageType) String() string {
	return msgTypeStringMap[m]
}
func (m MessageType) GetEmptyStruct() MessageLike {
	return msgTypeMap[m]
}

type Action int

const (
	Add Action = iota
	Del
	Upd
)

var actionMap = map[string]Action{
	"Add": Add,
	"Del": Del,
	"Upd": Upd,
}

func (a Action) String() string {
	return [...]string{"Add", "Del", "Upd"}[a]
}
func ActionByName(name string) Action {
	if a, ok := actionMap[name]; ok {
		return a
	}
	return -1
}

func (a Action) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a *Action) UnmarshalJSON(b []byte) error {
	var outStr string
	if err := json.Unmarshal(b, &outStr); err != nil {
		return err
	}
	*a = ActionByName(outStr)
	return nil
}

type ActionSubject int

const (
	Deployment ActionSubject = iota
	Pod
	Daemonset
	Service
)

var actionSubjectMap = map[string]ActionSubject{
	Deployment.String(): Deployment,
	Pod.String():        Pod,
	Daemonset.String():  Daemonset,
	Service.String():    Service,
}

func ActionSubjectByName(name string) ActionSubject {
	if a, ok := actionSubjectMap[name]; ok {
		return a
	}
	return -1
}
func (a ActionSubject) String() string {
	return [...]string{"Deployment", "Pod", "DaemonSet", "Service"}[a]
}

func (a ActionSubject) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a *ActionSubject) UnmarshalJSON(b []byte) error {
	var outStr string
	if err := json.Unmarshal(b, &outStr); err != nil {
		return err
	}
	*a = ActionSubjectByName(outStr)
	return nil
}

type Topic int

const (
	AppList Topic = iota
	AdvList
)

var topicMap = map[string]Topic{
	AppList.String(): AppList,
	AdvList.String(): AdvList,
}

func (t Topic) String() string {
	return [...]string{"AppList", "AdvList"}[t]
}
func topicByName(name string) Topic {
	if a, ok := topicMap[name]; ok {
		return a
	}
	return -1
}

func (t Topic) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
func (t *Topic) UnmarshalJSON(b []byte) error {
	var outStr string
	if err := json.Unmarshal(b, &outStr); err != nil {
		return err
	}
	*t = topicByName(outStr)
	return nil
}

type Message struct {
	Action        Action        `json:"Action"`
	ActionSubject ActionSubject `json:"ActionSubject"`
	Topic         Topic         `json:"Topic"`
}

type DeploymentMessage struct {
	Message `json:"Message"`
	Labels  map[string]string `json:"Labels"`
	Name    string            `json:"Name"`
}

type MessageLike interface {
	GetAction() Action
	GetActionSubject() ActionSubject
	GetTopic() Topic
	GetMsgType() MessageType
	SetAction(Action)
	SetActionSubject(ActionSubject)
	SetTopic(Topic)
}

type KubeLikeMsg interface {
	MessageLike
	GetName() string
	SetName(string)
	GetLabels() map[string]string
	SetLabels(map[string]string)
}

type Sender interface {
	SendMsg(c chan<- any)
	UpdateMsg(n string, l map[string]string, a Action)
}
type SendMessageHandler[T KubeLike] interface {
	MessageLike
	Sender
	MessageLoop(h HandlerChannels[T], appList *SafeAppSlice, ctx context.Context)
}

type RecvMessageHandler interface {
	// TODO: this
	KubeLikeMsg
	Recv()
	UpdateLoop(handleAction func(aMsg MessageLike, appList *SafeAppSlice, appCfg models.AppCfg) (string, map[string]string, error))
}

func (m DeploymentMessage) GetMsgType() MessageType {
	return msgTypeStructMap[&m]
}

func (m DeploymentMessage) GetLabels() map[string]string {
	return m.Labels
}
func (m *DeploymentMessage) SetLabels(l map[string]string) {
	m.Labels = l
}

func (m DeploymentMessage) GetName() string {
	return m.Name
}
func (m *DeploymentMessage) SetName(n string) {
	m.Name = n
}

func (m DeploymentMessage) GetTopic() Topic {
	return m.Topic
}
func (m *DeploymentMessage) SetTopic(t Topic) {
	m.Topic = t
}

func (m DeploymentMessage) GetActionSubject() ActionSubject {
	return m.ActionSubject
}
func (m *DeploymentMessage) SetActionSubject(a ActionSubject) {
	m.ActionSubject = a
}

func (m DeploymentMessage) GetAction() Action {
	return m.Action
}
func (m *DeploymentMessage) SetAction(a Action) {
	m.Action = a
}

func (m *DeploymentMessage) MessageLoop(h HandlerChannels[*appsv1.Deployment], appList *SafeAppSlice, ctx context.Context) {
CheckDeplLoop:
	for {
		select {
		case dplAdd := <-h.addChan:
			fmt.Printf("DEPLOYMENT ADDED: %s %s\n", dplAdd.GetName(), dplAdd.GetNamespace())
			m.UpdateMsg(dplAdd.GetName(), dplAdd.GetLabels(), Add)
			m.SendMsg(h.producerChan)

		case dplDel := <-h.delChan:
			fmt.Printf("DEPLOYMENT DELETED: %s %s\n", dplDel.GetName(), dplDel.GetNamespace())
			m.UpdateMsg(dplDel.GetName(), dplDel.GetLabels(), Del)
			m.SendMsg(h.producerChan)

		case dplUpd := <-h.updChan:
			fmt.Printf("DEPLOYMENT UPDATED: %s %s\n", dplUpd.GetName(), dplUpd.GetNamespace())
			needsUpdate, err := checkNeedsUpdate(AnApp{dplUpd.GetName(), dplUpd.GetLabels()}, appList)
			if err != nil {
				h.errChan <- err
			} else if needsUpdate {
				fmt.Println("\n\n NEEDS UPDATE \n\n")
				m.UpdateMsg(dplUpd.GetName(), dplUpd.GetLabels(), Upd)
				m.SendMsg(h.producerChan)
			}
		case e := <-h.errChan:
			fmt.Printf("CheckDeplLoop err == %s\n", e)
		case <-ctx.Done():
			fmt.Println("CheckDeplLoop All done!")
			break CheckDeplLoop
		}
	}

}

func (m *DeploymentMessage) UpdateMsg(n string, l map[string]string, a Action) {
	m.SetName(n)
	m.SetLabels(l)
	m.SetAction(a)
}

func (m DeploymentMessage) SendMsg(c chan<- any) {
	fmt.Printf("\nSENDING MESSAGE: \n\t%v\n", m)
	c <- m
}

func (m DeploymentMessage) Send(c chan<- any) {
	fmt.Printf("\nSENDING MESSAGE: \n\t%v\n", m)
	c <- m
}
func (m1 Message) Equal(m2 Message) bool {
	return m1.Action == m2.Action && m1.ActionSubject == m2.ActionSubject && m1.Topic == m2.Topic
}

func (d1 DeploymentMessage) Equal(d2 DeploymentMessage) bool {
	return d1.Name == d2.Name && maps.Equal(d1.Labels, d2.Labels) && d1.Message.Equal(d2.Message)
}
func NewDeploymentMessage(name string, labels map[string]string, action Action) DeploymentMessage {
	return DeploymentMessage{
		Message: AddDplBaseMessage[action.String()],
		Name:    name,
		Labels:  labels,
	}
}
func NewEmptyDeploymentMessage() *DeploymentMessage {
	return &DeploymentMessage{}
}
