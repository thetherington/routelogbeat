package beater

import "time"

type SyslogMessage struct {
	UUID       string     `json:"@UUID"`
	Tags       []string   `json:"tags"`
	Ecs        Ecs        `json:"ecs"`
	Timestamp  time.Time  `json:"@timestamp"`
	Host       Host       `json:"host"`
	Annotation Annotation `json:"annotation"`
	Log        Log        `json:"log"`
	Process    Process    `json:"process"`
	Device     Device     `json:"device"`
	Version    string     `json:"@version"`
	Event      Event      `json:"event"`
	Message    string     `json:"message"`
}
type Ecs struct {
	Version string `json:"version"`
}
type PortNames struct {
	Broadview string `json:"broadview"`
	Eng       string `json:"eng"`
}
type Host struct {
	IP   string `json:"ip"`
	Name string `json:"name"`
}
type Annotation struct {
	General General `json:"general"`
}
type General struct {
	DeviceName string `json:"device_name"`
	DeviceType string `json:"device_type"`
}
type Facility struct {
	Name string `json:"name"`
	Code int    `json:"code"`
}
type Severity struct {
	Name string `json:"name"`
	Code int    `json:"code"`
}
type Syslog struct {
	Facility Facility `json:"facility"`
	Severity Severity `json:"severity"`
	Priority string   `json:"priority"`
	Message  string   `json:"message"`
}
type Log struct {
	Syslog   Syslog `json:"syslog"`
	Original string `json:"original"`
}
type Process struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}
type Device struct {
	Timestamp time.Time `json:"timestamp"`
}
type Event struct {
	Original string    `json:"original"`
	Created  time.Time `json:"created"`
}
