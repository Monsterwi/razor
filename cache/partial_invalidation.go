package cache

// InvalidatePartialObject retires only the selected generation. A delayed failed
// fetch cannot fence a newer object or the newer object's active chunk writers.
func (c *Cache) InvalidatePartialObject(parent *FillToken, root []byte, selected *PartialObject) error {
	if selected.Revision == 0 {
		return ErrInvalidated
	}
	key := PartialObjectKey(root, selected.Origin, selected.Variant)
	token, err := c.DeriveFill(parent, key)
	if err != nil {
		return err
	}
	current, _, err := c.ReadPartialObject(key, selected.Origin, selected.Variant)
	if err != nil {
		return err
	}
	if current.Generation != selected.Generation || current.Revision != selected.Revision {
		return nil
	}
	// Check the captured directory identity again under the publication locks;
	// replacement or disk wrap between the read and retirement is a safe no-op.
	return c.invalidateFill(token, true)
}
