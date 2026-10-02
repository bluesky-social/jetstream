package storagefake

import (
	"cmp"
	"slices"
	"sync/atomic"
)

// maxDepth bounds a layer chain. Reads walk the chain, so a commit that
// would push past it flattens instead: O(n) once per maxDepth commits.
const maxDepth = 16

// layer is a copy-on-write map. A committed table is a frozen chain of
// layers; a write transaction stacks one mutable layer on top and publishes
// it at commit. Readers hold a frozen chain, which is how REPEATABLE READ
// snapshots cost nothing.
type layer[K cmp.Ordered, V any] struct {
	parent *layer[K, V]
	m      map[K]slot[V]
	depth  int

	// frozen layers memoize their flattened contents and sorted keys.
	frozen bool
	flat   atomic.Pointer[map[K]V]
	sorted atomic.Pointer[[]K]
}

type slot[V any] struct {
	v   V
	del bool
}

func newLayer[K cmp.Ordered, V any]() *layer[K, V] {
	return &layer[K, V]{m: map[K]slot[V]{}, frozen: true}
}

func (l *layer[K, V]) get(k K) (V, bool) {
	for c := l; c != nil; c = c.parent {
		if s, ok := c.m[k]; ok {
			if s.del {
				break
			}
			return s.v, true
		}
	}
	var zero V
	return zero, false
}

// child returns a mutable layer over l.
func (l *layer[K, V]) child() *layer[K, V] {
	return &layer[K, V]{parent: l, m: map[K]slot[V]{}, depth: l.depth + 1}
}

func (l *layer[K, V]) set(k K, v V) { l.m[k] = slot[V]{v: v} }

func (l *layer[K, V]) del(k K) { l.m[k] = slot[V]{del: true} }

// freeze makes a mutable layer committable, returning the layer to publish.
func (l *layer[K, V]) freeze() *layer[K, V] {
	if len(l.m) == 0 && l.parent != nil {
		return l.parent
	}
	if l.depth > maxDepth {
		flat := l.all()
		base := &layer[K, V]{m: make(map[K]slot[V], len(flat)), frozen: true}
		for k, v := range flat {
			base.m[k] = slot[V]{v: v}
		}
		return base
	}
	l.frozen = true
	return l
}

// all returns the flattened contents. Callers must not modify the result.
func (l *layer[K, V]) all() map[K]V {
	if l.frozen {
		if p := l.flat.Load(); p != nil {
			return *p
		}
	}
	var out map[K]V
	if l.parent == nil {
		out = make(map[K]V, len(l.m))
	} else {
		parent := l.parent.all()
		out = make(map[K]V, len(parent)+len(l.m))
		for k, v := range parent {
			out[k] = v
		}
	}
	for k, s := range l.m {
		if s.del {
			delete(out, k)
		} else {
			out[k] = s.v
		}
	}
	if l.frozen {
		l.flat.Store(&out)
	}
	return out
}

// keys returns the live keys in ascending order. Callers must not modify
// the result.
func (l *layer[K, V]) keys() []K {
	if l.frozen {
		if p := l.sorted.Load(); p != nil {
			return *p
		}
	}
	all := l.all()
	ks := make([]K, 0, len(all))
	for k := range all {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	if l.frozen {
		l.sorted.Store(&ks)
	}
	return ks
}

// values returns the live values in key order.
func (l *layer[K, V]) values() []V {
	all := l.all()
	ks := l.keys()
	out := make([]V, len(ks))
	for i, k := range ks {
		out[i] = all[k]
	}
	return out
}
