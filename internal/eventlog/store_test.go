package eventlog

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "log.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for i, typ := range []string{"a", "b", "c"} {
		id, err := st.Append(Event{UID: string(rune('x' + i)), Type: typ, Payload: []byte{byte(i)}, ArrivalNs: int64(100 + i)})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if id != int64(i+1) {
			t.Fatalf("id = %d, want %d", id, i+1)
		}
	}
	evs, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || evs[0].Type != "a" || evs[2].Type != "c" {
		t.Fatalf("list: %+v", evs)
	}
	if evs[1].ArrivalNs != 101 {
		t.Fatalf("arrival = %d", evs[1].ArrivalNs)
	}
}

func TestAppendDuplicateUID(t *testing.T) {
	st, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(Event{UID: "k1", Type: "a", Payload: []byte{1}, ArrivalNs: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(Event{UID: "k1", Type: "a", Payload: []byte{1}, ArrivalNs: 2}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	ok, err := st.Has("k1")
	if err != nil || !ok {
		t.Fatalf("Has(k1) = %v, %v", ok, err)
	}
	if ok, _ := st.Has("nope"); ok {
		t.Fatal("Has(nope) = true")
	}
	if n, _ := st.Len(); n != 1 {
		t.Fatalf("len = %d, want 1", n)
	}
}

func TestReopenPreservesOrder(t *testing.T) {
	db := filepath.Join(t.TempDir(), "log.db")
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := st.Append(Event{UID: string(rune('a' + i)), Type: "t", Payload: []byte{byte(i)}, ArrivalNs: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()

	st2, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	evs, err := st2.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 5 {
		t.Fatalf("reopened: %d events", len(evs))
	}
	for i, ev := range evs {
		if ev.Payload[0] != byte(i) {
			t.Fatalf("event %d payload = %v", i, ev.Payload)
		}
	}
}
