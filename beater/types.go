package beater

type Slabs []Slab

type Slab struct {
	Id     string
	Name   string
	Device string
	Output int
}

type Terminal struct {
	Id    string
	Name  string
	Label string
	Tag   string
}
