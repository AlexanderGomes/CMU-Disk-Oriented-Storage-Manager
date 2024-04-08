package storage

import (
	"container/list"
	"errors"
	"math"
	"time"
)

type LRUKReplacer struct {
	accessHistory *list.List
	frameToElem   map[FrameID]*list.Element
	k             int
}

type AccessEntry struct {
	FrameID     FrameID
	AccessTimes []time.Time
	Frequency   int
}

func (r *LRUKReplacer) RecordAccess(frameID FrameID) {
	accessTime := time.Now()

	if elem, ok := r.frameToElem[frameID]; ok {
		entry := elem.Value.(*AccessEntry)

		entry.AccessTimes = append(entry.AccessTimes, accessTime)
		entry.Frequency++

		r.accessHistory.MoveToFront(elem)
	} else {
		accessEntry := &AccessEntry{
			FrameID:     frameID,
			AccessTimes: []time.Time{accessTime},
			Frequency:   1,
		}
		elem := r.accessHistory.PushFront(accessEntry)
		r.frameToElem[frameID] = elem
	}
}

func (r *LRUKReplacer) Evict() (FrameID, error) {
	if r.accessHistory.Len() == 0 {
		return -1, nil
	}

	maxDistance := -1
	minFrequency := math.MaxInt64
	var evictedFrameID FrameID
	for frameID := range r.frameToElem {
		//look up on buffer pool if page is pinned
		distance, err := r.computeBackwardKDistance(frameID)
		if err != nil {
			return -1, err
		}

		elem := r.frameToElem[frameID]
		accessEntry := elem.Value.(*AccessEntry)

		if (distance > maxDistance) || (distance == maxDistance && accessEntry.Frequency < minFrequency) {
			maxDistance = distance
			minFrequency = accessEntry.Frequency
			evictedFrameID = frameID
		}
	}

	r.accessHistory.Remove(r.frameToElem[evictedFrameID])
	delete(r.frameToElem, evictedFrameID)

	return evictedFrameID, nil
}

func (r *LRUKReplacer) computeBackwardKDistance(frameID FrameID) (int, error) {
	elem, ok := r.frameToElem[frameID]
	if !ok {
		return 0, errors.New("frameID not found")
	}

	accessEntry := elem.Value.(*AccessEntry)
	accessTimes := accessEntry.AccessTimes
	if len(accessTimes) < r.k {
		return math.MaxInt32, nil
	}

	var lastAccessTime time.Time
	var kthAccessTime time.Time
	lastAccessTime = accessTimes[len(accessTimes)-1]
	kthAccessTime = accessTimes[len(accessTimes)-r.k]

	return int(lastAccessTime.Sub(kthAccessTime).Seconds()), nil
}

func NewLRUKReplacer(k int) *LRUKReplacer {
	return &LRUKReplacer{
		accessHistory: list.New(),
		frameToElem:   make(map[FrameID]*list.Element),
		k:             k,
	}
}
