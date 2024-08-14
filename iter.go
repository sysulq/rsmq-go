package rsmq

import "iter"

// Map2 returns an iterator over f applied to seq.
func Map2[KIn, VIn, KOut, VOut any](f func(KIn, VIn) (KOut, VOut), seq iter.Seq2[KIn, VIn]) iter.Seq2[KOut, VOut] {
	return func(yield func(KOut, VOut) bool) {
		for k, v := range seq {
			if !yield(f(k, v)) {
				return
			}
		}
	}
}
