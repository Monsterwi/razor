package cache

import (
	"fmt"
	"strings"
)

// ErrPurged revokes cache publication without changing the selected origin bytes.
var ErrPurged = fmt.Errorf("%w: purged", ErrInvalidated)

const purgeHistorySize = 64

type purgeEvent struct {
	selector string
	prefix   bool
}

func (event purgeEvent) matches(key []byte) bool {
	value := string(key)
	if event.prefix {
		return strings.HasPrefix(value, event.selector)
	}
	return value == event.selector || strings.HasPrefix(value, event.selector+"\nrange:")
}

// These history operations run under Stripe.mu, with writer -> stripe lock order.
func (od *OpenDir) recordPurge(selector []byte, prefix bool) {
	event := purgeEvent{string(selector), prefix}
	epoch := od.purgeEpoch.Add(1)
	od.purges[epoch%purgeHistorySize] = event
	// Known non-target writers need no history, even through a long purge burst.
	od.mutex.RLock()
	defer od.mutex.RUnlock()
	for _, entry := range od.entries {
		w := entry.Writer
		key := w.selector
		if len(w.fenceSelector) > 0 {
			key = w.fenceSelector
		}
		if !event.matches(key) {
			w.purgeEpoch = epoch
		}
	}
}

func (od *OpenDir) checkPurge(epoch uint64, key []byte) error {
	current := od.purgeEpoch.Load()
	if epoch == current {
		return nil
	}
	// shortcut: only 64 PURGEs are retained; older unbound fills lose publication
	// rights conservatively. Increase only if measured long-lived fills need it.
	if epoch > current || current-epoch > purgeHistorySize {
		return ErrPurged
	}
	for next := epoch + 1; next <= current; next++ {
		if od.purges[next%purgeHistorySize].matches(key) {
			return ErrPurged
		}
	}
	return nil
}
