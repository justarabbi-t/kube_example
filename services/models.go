package services

import "encoding/json"

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

type ActionSubject int

const (
	Deployment ActionSubject = iota
	Pod
	Daemonset
	Service
)

func (a *ActionSubject) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a ActionSubject) String() string {
	return [...]string{"Deployment", "Pod", "DaemonSet", "Service"}[a]
}

type Topic int

const (
	AppList Topic = iota
	AdvList
)

func (t Topic) String() string {
	return [...]string{"AppList", "AdvList"}[t]
}

func (t *Topic) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

type Message struct {
	Action        Action        `json:"Action"`
	ActionSubject ActionSubject `json:"ActionSubject"`
	Topic         Topic         `json:"Topic"`
}

func (m Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(m)
}
func (m Message) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &m)
}

type DeploymentMessage struct {
	Message `json:"Message"`
	Labels  map[string]string `json:"Labels"`
	Name    string            `json:"Name"`
}

func (m DeploymentMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(m)
}
func (m DeploymentMessage) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &m)
}

func (m DeploymentMessage) Send(c chan<- json.Marshaler) {
	c <- m
}

// type CiliumAdvMessage struct {
// 	Message `json:"Message"`
// 	Labels  map[string]string `json:"Labels"`
// 	Name    string            `json:"Name"`
// }
