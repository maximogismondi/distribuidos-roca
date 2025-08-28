package model

// ActionType represents the type of action to be performed.
type ActionType string

const (
	FilterAction     ActionType = "FILTER"
	OverviewerAction ActionType = "OVERVIEWER"
	MapperAction     ActionType = "MAPPER"
	JoinerAction     ActionType = "JOINER"
	ReducerAction    ActionType = "REDUCER"
	MergerAction     ActionType = "MERGER"
	TopperAction     ActionType = "TOPPER"
)
