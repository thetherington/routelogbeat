package magnumclient

type QueryTerminals struct {
	Terminals Terminals `graphql:"terminals(input: {filters: [{id: \"isTlr\", booleanValue: true}, {id: \"isDst\", booleanValue: true}, {id:\"isSub\", booleanValue: $isSub}, {id: \"tags\", listIncludes: [$tag]}]})"`
}

type SubscriptionTerminalsUpdated struct {
	TerminalsUpdated []Edge `graphql:"terminalsUpdated(input: {filters: [{id: \"isTlr\", booleanValue: true}, {id: \"isDst\", booleanValue: true}, {id:\"isSub\", booleanValue: $isSub}, {id: \"tags\", listIncludes: [$tag]}]})"`
}

type Terminals struct {
	TotalCount int
	Edges      []Edge `graphql:"edges(limit: $limit, offset: $offset)"`
}

type Edge struct {
	Id                        string
	Name                      string
	Tags                      []string
	IsSub                     bool
	IsDst                     bool
	Type                      string
	Port                      *Port
	NamesetNames              []NamesetName
	RouteableTerminalFragment `graphql:"... on RouteableTerminal"`
}

type Port struct {
	Id        string
	Name      string
	Device    Device
	Addresses []Addresses
}

type Device struct {
	Id   string
	Name string
}

type Addresses struct {
	Id           string
	Name         string
	Backup       bool
	StreamType   string
	EthernetPort *EthernetPort
}

type EthernetPort struct {
	Id string
}

type NamesetName struct {
	Id      string
	Name    string
	Nameset Nameset
}

type Nameset struct {
	Id   string
	Name string
}

type RouteableTerminalFragment struct {
	RoutedPhysicalSource *RoutedPhysicalSource
	SubscribedSource     *SubscribedSource
}

type RoutedPhysicalSource struct {
	Id           string
	Name         string
	IsSrc        bool
	Tags         []string
	NamesetNames []NamesetName
	Port         *Port
}

type SubscribedSource struct {
	Id           string
	Name         string
	IsSub        bool
	Tags         []string
	NamesetNames []NamesetName
}
