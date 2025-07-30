package model

// lo tengo que duplicar porque no puedo usar el struct de protocol.TaskIdentifier como clave, no es comparable
type TaskFragmentIdentifier struct {
	CreatorId          string
	TaskNumber         uint32
	TaskFragmentNumber uint32
	LastFragment       bool
}
