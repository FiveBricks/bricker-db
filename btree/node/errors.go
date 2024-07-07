package node

import "errors"

var ErrNoAvailableSpaceForInsert = errors.New("there is not enough available space for insert")
var ErrFailedToInsertData = errors.New("failed to insert data")
var ErrFailedToInsertKeyDataRef = errors.New("failed to insert key data ref")
var ErrFailedToInsertKeyPageRef = errors.New("failed to insert key page ref")
var ErrKeyRefAtIndexDoesNotExist = errors.New("Key data reference at given index does not exist")
