package kafka_talkers

import (
	"encoding/json"
	"fmt"
	"maps"
)

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

func (m DeploymentMessage) Send(c chan<- DeploymentMessage) {
	fmt.Printf("\nSENDING MESSAGE: \n\t%v\n", m)
	c <- m
}
func (m1 Message) Equal(m2 Message) bool {
	return m1.Action == m2.Action && m1.ActionSubject == m2.ActionSubject && m1.Topic == m2.Topic
}

func (d1 DeploymentMessage) Equal(d2 DeploymentMessage) bool {
	return d1.Name == d2.Name && maps.Equal(d1.Labels, d2.Labels) && d1.Message.Equal(d2.Message)
}
