package kafka_talkers

import (
	"encoding/json"
	"testing"
)

func TestMessageMarshalJSON(t *testing.T) {
	msg := Message{
		Action:        Add,
		ActionSubject: Pod,
		Topic:         AdvList,
	}
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("%s failed to Marshal msg err=%s", t.Name(), err)
	}
	daJson := string(jsonBytes)
	t.Logf("%s daJson=%s", t.Name(), daJson)
}

func TestMessageUnMarshalJSON(t *testing.T) {
	msg := Message{
		Action:        Add,
		ActionSubject: Pod,
		Topic:         AdvList,
	}
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("%s failed to Marshal msg err=%s", t.Name(), err)
	}
	newMsg := Message{}
	err = json.Unmarshal(jsonBytes, &newMsg)
	if err != nil {
		t.Fatalf("%s failed to Unmarshal msg err=%s", t.Name(), err)
	}
	t.Logf("%s newMsg=%v", t.Name(), newMsg)
	if !newMsg.Equal(msg) {
		t.Errorf("Unmarshaled newMsg=%v != marshaled msg=%v", newMsg, msg)
	}

}

func TestDeploymentMessageMarshalJSON(t *testing.T) {

	msg := DeploymentMessage{
		Message{
			Action:        Add,
			ActionSubject: Deployment,
			Topic:         AppList,
		},
		map[string]string{"Hi": "bob"},
		"Hi Bob",
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("%s failed to Marshal msg err=%s", t.Name(), err)
	}
	daJson := string(jsonBytes)
	t.Logf("%s daJson=%s", t.Name(), daJson)
}
func TestDeploymentMessageUnMarshalJSON(t *testing.T) {

	msg := DeploymentMessage{
		Message{
			Action:        Add,
			ActionSubject: Deployment,
			Topic:         AppList,
		},
		map[string]string{"Hi": "bob"},
		"Hi Bob",
	}
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("%s failed to Marshal msg err=%s", t.Name(), err)
	}
	newMsg := DeploymentMessage{}
	err = json.Unmarshal(jsonBytes, &newMsg)
	if err != nil {
		t.Fatalf("%s failed to Unmarshal msg err=%s", t.Name(), err)
	}
	t.Logf("%s newMsg=%v", t.Name(), newMsg)
	if !newMsg.Equal(msg) {
		t.Errorf("Unmarshaled newMsg=%v != marshaled msg=%v", newMsg, msg)
	}
}
