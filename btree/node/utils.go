package node

func FindPositionForKey(node Node, key uint32) (bool, uint32, error) {
	start := uint32(0)
	end := node.GetElementsCount()
	for start < end {
		middle := (start + end) / 2
		middleKeyDataRef, err := node.GetKeyRefeferenceByIndex(middle)
		if err != nil {
			return false, 0, err
		}

		if middleKeyDataRef.GetKey() == key {
			return true, middle, nil
		}

		if middleKeyDataRef.GetKey() < key {
			start = middle + 1
		} else {
			end = middle
		}
	}

	return false, start, nil
}
