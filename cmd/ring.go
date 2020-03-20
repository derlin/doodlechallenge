// from https://github.com/ericaro/ringbuffer/blob/master/ringbuffer.go
package main

//Ring is a basic implementation of a circular buffer http://en.wikipedia.org/wiki/Circular_buffer
// or Ring Buffer
type Ring struct {
	head, size int
	buf        []Window
}

//New creates a new, empty ring buffer.
func NewRing(capacity int) (b *Ring) {
	return &Ring{
		buf:  make([]Window, capacity),
		head: -1,
	}
}

// Remove 'count' items from the ring's tail.
//
// If count is greater than the actual ring's size, the ring size is reset to zero.
func (b *Ring) Remove(count int) {
	if count <= 0 {
		return
	}
	b.size -= count
	if b.size <= 0 {
		b.size = 0
		b.head = -1 //small trick to mark as empty
	}
	return
}

//Get returns the value in the ring.
//
//   Get(0) //retrieve the head
//   Get(size-1) //is the oldest
//   Get(-1) //is the oldest too
//
func (b *Ring) Get(i int) *Window {
	if b.size == 0 {
		return nil
	}
	position := Index(i, b.head, b.size, len(b.buf))
	return &b.buf[position]
}

//Capacity is the max size permitted
func (b *Ring) Capacity() int {
	return len(b.buf)
}

//Size returns the ring's size.
func (b *Ring) Size() int {
	return b.size
}

//add 'val' at the Ring's head, it also increases its size.
//If the capacity is exhausted (size == capacity) an error is returned.
func (b *Ring) Add(ts int) *Window {
	if b.size >= len(b.buf) {
		return nil
	}

	next := Next(1, b.head, len(b.buf))
	b.buf[next].Reset(ts)
	b.head = next
	b.size++ // increase the inner size
	return &b.buf[next]
}

// Next computes the next index for a ring buffer
func Next(i, latest, capacity int) int {
	n := (latest + i) % capacity
	if n < 0 {
		n += capacity
	}
	return n
}

//Index computes absolute position of a ring buffer index.
//
// i, is the ring's index.
//
// head, is the absolute index of the ring's head
//
// size, is the ring' size
//
// capacity is the buffer's capacity.
//
func Index(i, head, size, capacity int) int {
	// size=0 is a failure.
	if size == 0 {
		return -1
	}
	// first fold i values into  ]-size , size[
	i = i % size
	// then translate negative parts
	if i < 0 {
		i += size
	}

	// this way -1 is interpreted as size-1 etc.

	// now I've got the real i>=0
	// actual theoretical index is simply
	// last write minus the required offset.
	// last write is lastest
	// offset is i, because i==0 means exactly the last written.
	//
	pos := head - i

	//pos might be negative. this is the actual index in the ring buffer.
	// if head = 0, previous read is at len(buf)-1
	// if head == 0 (and i was zero), pos=-1 (as the above calculation)
	//so this is the same as before, negative indexes are added the actual size
	for pos < 0 {
		pos += capacity
	}

	// yehaa, pos is the head position.
	return pos
}
