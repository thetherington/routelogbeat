package querysubscriptionterminals

type QueryTerminals struct {
	Terminals Terminals `graphql:"terminals(input: {filters: [{id: \"isTlr\", booleanValue: true}, {id: \"isDst\", booleanValue: true}, {id:\"isSub\", booleanValue: true}, {id: \"tags\", listIncludes: [$tag]}]})"`
}

type Terminals struct {
	TotalCount int
	Edges      []Edge `graphql:"edges(limit: $limit, offset: $offset)"`
}

type Edge struct {
	Id           string
	Name         string
	NamesetNames []NamesetName
}

type NamesetName struct {
	Name    string
	Nameset Nameset
}

type Nameset struct {
	Name string
}

// findNamesetValueByName searches for a nameset value by its name within a slice of NamesetName.
// If the name is found, it returns the corresponding value; otherwise, it returns the default value.
func (e *Edge) FindNamesetValueByName(s string, defaultValue string) string {
	for _, n := range e.NamesetNames {
		if n.Nameset.Name == s {
			return n.Name
		}
	}

	return defaultValue
}
