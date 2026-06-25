package attprop

// testhooks.go exposes minimal, eventloop-safe read accessors for tests in
// other packages (e.g. node) that cannot reach unexported Manager state. These
// are read-only and route through the eventloop goroutine so they observe a
// consistent snapshot without a lock, matching the single-owner discipline of
// the rest of the Manager. Not used by production code.

// ValidatedCount returns the number of distinct committee positions validated
// for slot, summed across every fork bucket. It runs on the eventloop goroutine,
// so it is safe to call concurrently with a running Manager.
func (m *Manager) ValidatedCount(slot int) int {
	done := make(chan int, 1)
	m.post(funcEvent{fn: func() {
		ss := m.getSlotState(slot)
		if ss == nil {
			done <- 0
			return
		}
		n := 0
		for _, b := range ss.buckets {
			n += len(b.validated)
		}
		done <- n
	}})
	return <-done
}
