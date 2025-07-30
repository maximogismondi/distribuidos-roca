package model

type Movie struct {
	Id            string
	ProdCountries []string
	Title         string
	Revenue       uint64
	Budget        uint64
	Overview      string
	ReleaseYear   uint32
	Genres        []string
}
