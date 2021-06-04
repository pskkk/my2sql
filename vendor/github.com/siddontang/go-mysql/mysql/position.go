package mysql

import (
	"fmt"
)

// For binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}

// Compare 比较两个binlog 先后顺序，若只有一个binlog 则比较postion先后顺序，是否正确
func (p Position) Compare(o Position) int {
	// First compare binlog name
	if p.Name > o.Name {
		return 1
	} else if p.Name < o.Name {
		return -1
	} else {
		// Same binlog file, compare position
		if p.Pos > o.Pos {
			return 1
		} else if p.Pos < o.Pos {
			return -1
		} else {
			return 0
		}
	}
}

func (p Position) String() string {
	return fmt.Sprintf("(%s, %d)", p.Name, p.Pos)
}
