package node

func FindPositionForKey(node Node, key uint32) (bool, uint32, error) {
	start := uint32(0)
	end := node.GetElementsCount()
	for start < end {
		middle := (start + end) / 2
		middleKeyRef, err := node.GetKeyRefeferenceByIndex(middle)
		if err != nil {
			return false, 0, err
		}

		if middleKeyRef.GetKey() == key {
			return true, middle, nil
		}

		if middleKeyRef.GetKey() < key {
			start = middle + 1
		} else {
			end = middle
		}
	}

	return false, start, nil
}

func FindPositionForKeyInRefs[T KeyReference](key uint32, refs []T) (bool, uint32) {
	start := uint32(0)
	end := uint32(len(refs))
	for start < end {
		middle := (start + end) / 2
		middleKeyRef := refs[middle]

		if middleKeyRef.GetKey() == key {
			return true, middle
		}

		if middleKeyRef.GetKey() < key {
			start = middle + 1
		} else {
			end = middle
		}
	}

	return false, start
}
