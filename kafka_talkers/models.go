package kafka_talkers

import (
	"encoding/json"
	"fmt"
	"maps"
)

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
	GetLabels() map[string]string
	GetName() string
	GetMsgType() MessageType
	SetAction(Action)
	SetActionSubject(ActionSubject)
	SetTopic(Topic)
	SetLabels(map[string]string)
	SetName(string)
}

type Sender interface {
	SendMsg(c chan<- any)
	UpdateMsg(n string, l map[string]string, a Action)
}
type MessageHandler interface {
	MessageLike
	Sender
	MessageLoop()
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

func (m *DeploymentMessage) MessageLoop() {
	// for later

}

//	func SendMessage[T any](c chan<- T, m Sender) {
//		m.Send(any(c))
//	}
func (m *DeploymentMessage) UpdateMsg(n string, l map[string]string, a Action) {
	m.Name = n
	m.Labels = l
	m.Action = a
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
