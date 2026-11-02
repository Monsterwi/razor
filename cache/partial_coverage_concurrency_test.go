package cache

import (
	"sync"
	"testing"
)

func TestPartialCoverageRemainsReadableDuringPublication(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	for index := int64(0); index < 4; index++ {
		publishPartialFixture(t, c, m, root, parent, info, index)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for i := 0; i < 256; i++ {
			if err := c.PublishPartialChunk(t.Context(), parent, root, m, int64(i%4), info); err != nil {
				t.Errorf("republish completed chunk: %v", err)
				return
			}
		}
	})
	for reader := 0; reader < 4; reader++ {
		workers.Go(func() {
			<-start
			for i := 0; i < 256; i++ {
				coverage, err := c.PartialCoverage(root, m, 0)
				if err != nil || coverage.Bits != 15 {
					t.Errorf("completed coverage disappeared during publication: bits=%#x err=%v", coverage.Bits, err)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
}
