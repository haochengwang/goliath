package utils

import (
	"strings"
	"sync"
)

type StringBuilder struct {
	builder strings.Builder
	mutex   sync.Mutex
}

func (s *StringBuilder) WriteString(str string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.builder.WriteString(str)
}

func (s *StringBuilder) WriteByte(c byte) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.builder.WriteByte(c)
}

func (s *StringBuilder) String() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.builder.String()
}
