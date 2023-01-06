package kv

type Item struct {
	key     string
	val     []byte
	timeout string
}

func (i *Item) Key() string {
	return i.key
}

func (i *Item) Value() []byte {
	return i.val
}

func (i *Item) Timeout() string {
	return i.timeout
}
