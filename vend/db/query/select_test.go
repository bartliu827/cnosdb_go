package query_test

import (
	"context"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"reflect"
	"testing"
	"time"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

func TestSelect(t *testing.T) {
	for _, tt := range []struct {
		name   string
		q      string
		typ    cnosql.DataType
		fields map[string]cnosql.DataType
		expr   string
		itrs   []query.Iterator
		rows   []query.Row
		now    time.Time
		err    string
	}{
		{
			name: "Min",
			q:    `SELECT min(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
			},
		},
		{
			name: "Distinct_Float",
			q:    `SELECT distinct(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: 19},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
			},
		},
		{
			name: "Distinct_Integer",
			q:    `SELECT distinct(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: 19},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(10)}},
			},
		},
		{
			name: "Distinct_Unsigned",
			q:    `SELECT distinct(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: 19},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(10)}},
			},
		},
		{
			name: "Distinct_String",
			q:    `SELECT distinct(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.String,
			itrs: []query.Iterator{
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: "a"},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: "b"},
				}},
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: "c"},
				}},
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: "b"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: "d"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: "d"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: "d"},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"a"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"b"}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"d"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{"c"}},
			},
		},
		{
			name: "Distinct_Boolean",
			q:    `SELECT distinct(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: false},
				}},
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: false},
				}},
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: true},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{true}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{false}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{false}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{true}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{false}},
			},
		},
		{
			name: "Mean_Float",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{3.2}},
			},
		},
		{
			name: "Mean_Integer",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			expr: `mean(value::integer)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{3.2}},
			},
		},
		{
			name: "Mean_Unsigned",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			expr: `mean(value::Unsigned)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{3.2}},
			},
		},
		{
			name: "Mean_String",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.String,
			itrs: []query.Iterator{&StringIterator{}},
			err:  `unsupported mean iterator type: *query_test.StringIterator`,
		},
		{
			name: "Mean_Boolean",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{&BooleanIterator{}},
			err:  `unsupported mean iterator type: *query_test.BooleanIterator`,
		},
		{
			name: "Median_Float",
			q:    `SELECT median(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(3)}},
			},
		},
		{
			name: "Median_Integer",
			q:    `SELECT median(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(3)}},
			},
		},
		{
			name: "Median_Unsigned",
			q:    `SELECT median(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{19.5}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{2.5}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(3)}},
			},
		},
		{
			name: "Median_String",
			q:    `SELECT median(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.String,
			itrs: []query.Iterator{&StringIterator{}},
			err:  `unsupported median iterator type: *query_test.StringIterator`,
		},
		{
			name: "Median_Boolean",
			q:    `SELECT median(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{&BooleanIterator{}},
			err:  `unsupported median iterator type: *query_test.BooleanIterator`,
		},
		{
			name: "Mode_Float",
			q:    `SELECT mode(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(1)}},
			},
		},
		{
			name: "Mode_Integer",
			q:    `SELECT mode(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 54 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(1)}},
			},
		},
		{
			name: "Mode_Unsigned",
			q:    `SELECT mode(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 54 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(1)}},
			},
		},
		{
			name: "Mode_String",
			q:    `SELECT mode(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.String,
			itrs: []query.Iterator{
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: "a"},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: "a"},
				}},
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: "cxxx"},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 6 * Second, Value: "zzzz"},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 7 * Second, Value: "zzzz"},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 8 * Second, Value: "zxxx"},
				}},
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: "b"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: "d"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: "d"},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: "d"},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"a"}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"d"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{"zzzz"}},
			},
		},
		{
			name: "Mode_Boolean",
			q:    `SELECT mode(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 2 * Second, Value: false},
				}},
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 6 * Second, Value: false},
				}},
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: true},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{false}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{true}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{true}},
			},
		},
		{
			name: "Top_NoTags_Float",
			q:    `SELECT top(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(19)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(5)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(4)}},
			},
		},
		{
			name: "Top_NoTags_Integer",
			q:    `SELECT top(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(19)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(10)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(5)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(4)}},
			},
		},
		{
			name: "Top_NoTags_Unsigned",
			q:    `SELECT top(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(19)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(10)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(5)}},
				{Time: 53 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(4)}},
			},
		},
		{
			name: "Top_Tags_Float",
			q:    `SELECT top(value::float, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Float,
			expr: `max(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20), "A"}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10), "B"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100), "A"}},
				{Time: 53 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5), "B"}},
			},
		},
		{
			name: "Top_Tags_Integer",
			q:    `SELECT top(value::integer, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Integer,
			expr: `max(value::integer)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20), "A"}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(10), "B"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(100), "A"}},
				{Time: 53 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(5), "B"}},
			},
		},
		{
			name: "Top_Tags_Unsigned",
			q:    `SELECT top(value::Unsigned, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Unsigned,
			expr: `max(value::Unsigned)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20), "A"}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(10), "B"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(100), "A"}},
				{Time: 53 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(5), "B"}},
			},
		},
		{
			name: "Top_GroupByTags_Float",
			q:    `SELECT top(value::float, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Float,
			expr: `max(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{float64(19), "A"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{float64(20), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{float64(100), "A"}},
			},
		},
		{
			name: "Top_GroupByTags_Integer",
			q:    `SELECT top(value::integer, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{int64(19), "A"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{int64(20), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{int64(100), "A"}},
			},
		},
		{
			name: "Top_GroupByTags_Unsigned",
			q:    `SELECT top(value::Unsigned, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 9 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{uint64(19), "A"}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{uint64(20), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{uint64(100), "A"}},
			},
		},
		{
			name: "Top_AuxFields_Float",
			q:    `SELECT top(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Float,
				"p2": cnosql.Float,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{float64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{float64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{float64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{float64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 2 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3), float64(4), "ccc"}},
				{Time: 3 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(4), float64(5), "ddd"}},
			},
		},
		{
			name: "Top_AuxFields_Integer",
			q:    `SELECT top(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Integer,
				"p2": cnosql.Integer,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{int64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{int64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{int64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{int64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 2 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3), int64(4), "ccc"}},
				{Time: 3 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4), int64(5), "ddd"}},
			},
		},
		{
			name: "Top_AuxFields_Unsigned",
			q:    `SELECT top(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Unsigned,
				"p2": cnosql.Unsigned,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{uint64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{uint64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{uint64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{uint64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 2 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(3), uint64(4), "ccc"}},
				{Time: 3 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(4), uint64(5), "ddd"}},
			},
		},
		{
			name: "Bottom_NoTags_Float",
			q:    `SELECT bottom(value::float, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(3)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(1)}},
				{Time: 51 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(2)}},
			},
		},
		{
			name: "Bottom_NoTags_Integer",
			q:    `SELECT bottom(value::integer, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(3)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(1)}},
				{Time: 51 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(2)}},
			},
		},
		{
			name: "Bottom_NoTags_Unsigned",
			q:    `SELECT bottom(value::Unsigned, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(3)}},
				{Time: 31 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(100)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(1)}},
				{Time: 51 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(2)}},
			},
		},
		{
			name: "Bottom_Tags_Float",
			q:    `SELECT bottom(value::float, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Float,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10), "B"}},
				{Time: 10 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1), "B"}},
			},
		},
		{
			name: "Bottom_Tags_Integer",
			q:    `SELECT bottom(value::integer, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(10), "B"}},
				{Time: 10 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(2), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(100), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(1), "B"}},
			},
		},
		{
			name: "Bottom_Tags_Unsigned",
			q:    `SELECT bottom(value::Unsigned, station::tag, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(10), "B"}},
				{Time: 10 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(2), "A"}},
				{Time: 31 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(100), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1), "B"}},
			},
		},
		{
			name: "Bottom_GroupByTags_Float",
			q:    `SELECT bottom(value::float, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Float,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{float64(2), "A"}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{float64(3), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{float64(1), "B"}},
			},
		},
		{
			name: "Bottom_GroupByTags_Integer",
			q:    `SELECT bottom(value::float, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Integer,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{int64(2), "A"}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{int64(3), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{int64(1), "B"}},
			},
		},
		{
			name: "Bottom_GroupByTags_Unsigned",
			q:    `SELECT bottom(value::float, station::tag, 1) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			typ:  cnosql.Unsigned,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			rows: []query.Row{
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=east")}, Values: []interface{}{uint64(2), "A"}},
				{Time: 11 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{uint64(3), "A"}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("region=west")}, Values: []interface{}{uint64(1), "B"}},
			},
		},
		{
			name: "Bottom_AuxFields_Float",
			q:    `SELECT bottom(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Float,
				"p2": cnosql.Float,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{float64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{float64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{float64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{float64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1), float64(2), "aaa"}},
				{Time: 1 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2), float64(3), "bbb"}},
			},
		},
		{
			name: "Bottom_AuxFields_Integer",
			q:    `SELECT bottom(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Integer,
				"p2": cnosql.Integer,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{int64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{int64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{int64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{int64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(1), int64(2), "aaa"}},
				{Time: 1 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(2), int64(3), "bbb"}},
			},
		},
		{
			name: "Bottom_AuxFields_Unsigned",
			q:    `SELECT bottom(p1, 2), p2, p3 FROM air`,
			fields: map[string]cnosql.DataType{
				"p1": cnosql.Unsigned,
				"p2": cnosql.Unsigned,
				"p3": cnosql.String,
			},
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 1, Aux: []interface{}{uint64(2), "aaa"}},
					{Name: "air", Time: 1 * Second, Value: 2, Aux: []interface{}{uint64(3), "bbb"}},
					{Name: "air", Time: 2 * Second, Value: 3, Aux: []interface{}{uint64(4), "ccc"}},
					{Name: "air", Time: 3 * Second, Value: 4, Aux: []interface{}{uint64(5), "ddd"}},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1), uint64(2), "aaa"}},
				{Time: 1 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(2), uint64(3), "bbb"}},
			},
		},
		{
			name: "Fill_Null_Float",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(null)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Number_Float",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(1)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
			},
		},
		{
			name: "Fill_Previous_Float",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(previous)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
			},
		},
		{
			name: "Fill_Previous_Float_Two_Series",
			q:    `SELECT last(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(previous)`,
			typ:  cnosql.Float,
			expr: `last(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 30 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("station=A"), Time: 40 * Second, Value: 30},
					{Name: "air", Tags: ParseTags("station=B"), Time: 30 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("station=B"), Time: 40 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(30)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(30)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(1)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(2)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(2)}},
			},
		},
		{
			name: "Fill_Linear_Float_One",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("station=A"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(3)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Linear_Float_Many",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("station=A"), Time: 62 * Second, Value: 7},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(3)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(5)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(6)}},
				{Time: 60 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(7)}},
			},
		},
		{
			name: "Fill_Linear_Float_MultipleSeries",
			q:    `SELECT mean(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("station=B"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Linear_Integer_One",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Integer,
			expr: `max(value::integer)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("station=A"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(1)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Linear_Integer_Many",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:20Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Integer,
			expr: `max(value::integer)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("station=A"), Time: 72 * Second, Value: 10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(1)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(5)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(7)}},
				{Time: 60 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(8)}},
				{Time: 70 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(10)}},
			},
		},
		{
			name: "Fill_Linear_Integer_MultipleSeries",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Integer,
			expr: `max(value::integer)`,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("station=B"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Linear_Unsigned_One",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Unsigned,
			expr: `max(value::Unsigned)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("station=A"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(1)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Fill_Linear_Unsigned_Many",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:20Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Unsigned,
			expr: `max(value::Unsigned)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("station=A"), Time: 72 * Second, Value: 10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(1)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(5)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(7)}},
				{Time: 60 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(8)}},
				{Time: 70 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(10)}},
			},
		},
		{
			name: "Fill_Linear_Unsigned_MultipleSeries",
			q:    `SELECT max(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY station, time(10s) fill(linear)`,
			typ:  cnosql.Unsigned,
			expr: `max(value::Unsigned)`,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("station=A"), Time: 12 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("station=B"), Time: 32 * Second, Value: 4},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(2)}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{nil}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 20 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(4)}},
				{Time: 40 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{nil}},
			},
		},
		{
			name: "Stddev_Float",
			q:    `SELECT stddev(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{query.NullFloat}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{query.NullFloat}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{1.5811388300841898}},
			},
		},
		{
			name: "Stddev_Integer",
			q:    `SELECT stddev(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{query.NullFloat}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{query.NullFloat}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{1.5811388300841898}},
			},
		},
		{
			name: "Stddev_Unsigned",
			q:    `SELECT stddev(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{0.7071067811865476}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{query.NullFloat}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{query.NullFloat}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{1.5811388300841898}},
			},
		},
		{
			name: "Spread_Float",
			q:    `SELECT spread(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(1)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(0)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(0)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(4)}},
			},
		},
		{
			name: "Spread_Integer",
			q:    `SELECT spread(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(1)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(1)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(0)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(0)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(4)}},
			},
		},
		{
			name: "Spread_Unsigned",
			q:    `SELECT spread(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 1},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 5},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(1)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(1)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(0)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(0)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(4)}},
			},
		},
		{
			name: "Percentile_Float",
			q:    `SELECT percentile(value, 90) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 9},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 8},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 7},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 54 * Second, Value: 6},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 55 * Second, Value: 5},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 56 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 57 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 58 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 59 * Second, Value: 1},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(3)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(9)}},
			},
		},
		{
			name: "Percentile_Integer",
			q:    `SELECT percentile(value, 90) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 9},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 8},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 7},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 54 * Second, Value: 6},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 55 * Second, Value: 5},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 56 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 57 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 58 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 59 * Second, Value: 1},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(20)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(3)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(9)}},
			},
		},
		{
			name: "Percentile_Unsigned",
			q:    `SELECT percentile(value, 90) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 50 * Second, Value: 10},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 51 * Second, Value: 9},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 52 * Second, Value: 8},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 53 * Second, Value: 7},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 54 * Second, Value: 6},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 55 * Second, Value: 5},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 56 * Second, Value: 4},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 57 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 58 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 59 * Second, Value: 1},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(20)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(3)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(10)}},
				{Time: 50 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(9)}},
			},
		},
		{
			name: "Sample_Float",
			q:    `SELECT sample(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 5 * Second, Value: 10},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 10 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 15 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(19)}},
				{Time: 15 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(2)}},
			},
		},
		{
			name: "Sample_Integer",
			q:    `SELECT sample(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 5 * Second, Value: 10},
				}},
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 10 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 15 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{int64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(19)}},
				{Time: 15 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{int64(2)}},
			},
		},
		{
			name: "Sample_Unsigned",
			q:    `SELECT sample(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 5 * Second, Value: 10},
				}},
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 10 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 15 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{uint64(10)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(19)}},
				{Time: 15 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{uint64(2)}},
			},
		},
		{
			name: "Sample_String",
			q:    `SELECT sample(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.String,
			itrs: []query.Iterator{
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: "a"},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 5 * Second, Value: "b"},
				}},
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 10 * Second, Value: "c"},
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 15 * Second, Value: "d"},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"a"}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{"b"}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{"c"}},
				{Time: 15 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{"d"}},
			},
		},
		{
			name: "Sample_Boolean",
			q:    `SELECT sample(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: true},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 5 * Second, Value: false},
				}},
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 10 * Second, Value: false},
					{Name: "air", Tags: ParseTags("region=east,station=B"), Time: 15 * Second, Value: true},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{true}},
				{Time: 5 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{false}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{false}},
				{Time: 15 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{true}},
			},
		},
		//{
		//	name: "Raw",
		//	q:    `SELECT v1::float, v2::float FROM air`,
		//	itrs: []query.Iterator{
		//		&FloatIterator{Points: []query.FloatPoint{
		//			{Time: 0, Aux: []interface{}{float64(1), nil}},
		//			{Time: 1, Aux: []interface{}{nil, float64(2)}},
		//			{Time: 5, Aux: []interface{}{float64(3), float64(4)}},
		//		}},
		//	},
		//	points: [][]query.Point{
		//		{
		//			&query.FloatPoint{Time: 0, Value: 1},
		//			&query.FloatPoint{Time: 0, Nil: true},
		//		},
		//		{
		//			&query.FloatPoint{Time: 1, Nil: true},
		//			&query.FloatPoint{Time: 1, Value: 2},
		//		},
		//		{
		//			&query.FloatPoint{Time: 5, Value: 3},
		//			&query.FloatPoint{Time: 5, Value: 4},
		//		},
		//	},
		//},
		{
			name: "ParenExpr_Min",
			q:    `SELECT (min(value)) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(100)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
			},
		},
		{
			name: "ParenExpr_Distinct",
			q:    `SELECT (distinct(value)) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 1 * Second, Value: 19},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 11 * Second, Value: 2},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 12 * Second, Value: 2},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(19)}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(2)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(10)}},
			},
		},
		{
			name: "Derivative_Float",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.25)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-4)}},
			},
		},
		{
			name: "Derivative_Integer",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.25)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-4)}},
			},
		},
		{
			name: "Derivative_Unsigned",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.25)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-4)}},
			},
		},
		{
			name: "Derivative_Desc_Float",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 0 * Second, Value: 20},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(4)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.25)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.5)}},
			},
		},
		{
			name: "Derivative_Desc_Integer",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 0 * Second, Value: 20},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(4)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.25)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.5)}},
			},
		},
		{
			name: "Derivative_Desc_Unsigned",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 0 * Second, Value: 20},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(4)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.25)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2.5)}},
			},
		},
		{
			name: "Derivative_Duplicate_Float",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
			},
		},
		{
			name: "Derivative_Duplicate_Integer",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
			},
		},
		{
			name: "Derivative_Duplicate_Unsigned",
			q:    `SELECT derivative(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-2.5)}},
			},
		},
		{
			name: "Difference_Float",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-10)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-16)}},
			},
		},
		{
			name: "Difference_Integer",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-10)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(9)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-16)}},
			},
		},
		{
			name: "Difference_Unsigned",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(18446744073709551606)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(18446744073709551600)}},
			},
		},
		{
			name: "Difference_Duplicate_Float",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-10)}},
			},
		},
		{
			name: "Difference_Duplicate_Integer",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-10)}},
			},
		},
		{
			name: "Difference_Duplicate_Unsigned",
			q:    `SELECT difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(18446744073709551606)}},
			},
		},
		{
			name: "Non_Negative_Difference_Float",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 29},
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 16 * Second, Value: 39},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19)}},
				{Time: 16 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(36)}},
			},
		},
		{
			name: "Non_Negative_Difference_Integer",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 21},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(11)}},
			},
		},
		{
			name: "Non_Negative_Difference_Unsigned",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 21},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(11)}},
			},
		},
		{
			name: "Non_Negative_Difference_Duplicate_Float",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 30},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 10},
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 16 * Second, Value: 40},
					{Name: "air", Time: 16 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 16 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(30)}},
			},
		},
		{
			name: "Non_Negative_Difference_Duplicate_Integer",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 30},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 10},
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 16 * Second, Value: 40},
					{Name: "air", Time: 16 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 16 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(30)}},
			},
		},
		{
			name: "Non_Negative_Difference_Duplicate_Unsigned",
			q:    `SELECT non_negative_difference(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
					{Name: "air", Time: 8 * Second, Value: 30},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 10},
					{Name: "air", Time: 12 * Second, Value: 3},
					{Name: "air", Time: 16 * Second, Value: 40},
					{Name: "air", Time: 16 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 16 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(30)}},
			},
		},
		{
			name: "Elapsed_Float",
			q:    `SELECT elapsed(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 11 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 11 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			name: "Elapsed_Integer",
			q:    `SELECT elapsed(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 11 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 11 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			name: "Elapsed_Unsigned",
			q:    `SELECT elapsed(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 11 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 11 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			name: "Elapsed_String",
			q:    `SELECT elapsed(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.String,
			itrs: []query.Iterator{
				&StringIterator{Points: []query.StringPoint{
					{Name: "air", Time: 0 * Second, Value: "a"},
					{Name: "air", Time: 4 * Second, Value: "b"},
					{Name: "air", Time: 8 * Second, Value: "c"},
					{Name: "air", Time: 11 * Second, Value: "d"},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 11 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			name: "Elapsed_Boolean",
			q:    `SELECT elapsed(value, 1s) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Boolean,
			itrs: []query.Iterator{
				&BooleanIterator{Points: []query.BooleanPoint{
					{Name: "air", Time: 0 * Second, Value: true},
					{Name: "air", Time: 4 * Second, Value: false},
					{Name: "air", Time: 8 * Second, Value: false},
					{Name: "air", Time: 11 * Second, Value: true},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(4)}},
				{Time: 11 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(3)}},
			},
		},
		{
			name: "Integral_Float",
			q:    `SELECT integral(value) FROM air`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 10 * Second, Value: 20},
					{Name: "air", Time: 15 * Second, Value: 10},
					{Name: "air", Time: 20 * Second, Value: 0},
					{Name: "air", Time: 30 * Second, Value: -10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(50)}},
			},
		},
		{
			name: "Integral_Duplicate_Float",
			q:    `SELECT integral(value) FROM air`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 5 * Second, Value: 10},
					{Name: "air", Time: 5 * Second, Value: 30},
					{Name: "air", Time: 10 * Second, Value: 40},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(250)}},
			},
		},
		{
			name: "Integral_Float_GroupByTime",
			q:    `SELECT integral(value) FROM air WHERE time > 0s AND time < 60s GROUP BY time(20s)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 10 * Second, Value: 20},
					{Name: "air", Time: 15 * Second, Value: 10},
					{Name: "air", Time: 20 * Second, Value: 0},
					{Name: "air", Time: 30 * Second, Value: -10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
				{Time: 20 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-50)}},
			},
		},
		{
			name: "Integral_Float_InterpolateGroupByTime",
			q:    `SELECT integral(value) FROM air WHERE time > 0s AND time < 60s GROUP BY time(20s)`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 10 * Second, Value: 20},
					{Name: "air", Time: 15 * Second, Value: 10},
					{Name: "air", Time: 25 * Second, Value: 0},
					{Name: "air", Time: 30 * Second, Value: -10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(112.5)}},
				{Time: 20 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-12.5)}},
			},
		},
		{
			name: "Integral_Integer",
			q:    `SELECT integral(value) FROM air`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 5 * Second, Value: 10},
					{Name: "air", Time: 10 * Second, Value: 0},
					{Name: "air", Time: 20 * Second, Value: -10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(50)}},
			},
		},
		{
			name: "Integral_Duplicate_Integer",
			q:    `SELECT integral(value, 2s) FROM air`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 5 * Second, Value: 10},
					{Name: "air", Time: 5 * Second, Value: 30},
					{Name: "air", Time: 10 * Second, Value: 40},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(125)}},
			},
		},
		{
			name: "Integral_Unsigned",
			q:    `SELECT integral(value) FROM air`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 5 * Second, Value: 10},
					{Name: "air", Time: 10 * Second, Value: 0},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
			},
		},
		{
			name: "Integral_Duplicate_Unsigned",
			q:    `SELECT integral(value, 2s) FROM air`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 5 * Second, Value: 10},
					{Name: "air", Time: 5 * Second, Value: 30},
					{Name: "air", Time: 10 * Second, Value: 40},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(125)}},
			},
		},
		{
			name: "MovingAverage_Float",
			q:    `SELECT moving_average(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(15)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(14.5)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(11)}},
			},
		},
		{
			name: "MovingAverage_Integer",
			q:    `SELECT moving_average(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(15)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(14.5)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(11)}},
			},
		},
		{
			name: "MovingAverage_Unsigned",
			q:    `SELECT moving_average(value, 2) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(15)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(14.5)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(11)}},
			},
		},
		{
			name: "CumulativeSum_Float",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(30)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(49)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(52)}},
			},
		},
		{
			name: "CumulativeSum_Integer",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(30)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(49)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(52)}},
			},
		},
		{
			name: "CumulativeSum_Unsigned",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 8 * Second, Value: 19},
					{Name: "air", Time: 12 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(30)}},
				{Time: 8 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(49)}},
				{Time: 12 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(52)}},
			},
		},
		{
			name: "CumulativeSum_Duplicate_Float",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Float,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(39)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(49)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(52)}},
			},
		},
		{
			name: "CumulativeSum_Duplicate_Integer",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Integer,
			itrs: []query.Iterator{
				&IntegerIterator{Points: []query.IntegerPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(39)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(49)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(52)}},
			},
		},
		{
			name: "CumulativeSum_Duplicate_Unsigned",
			q:    `SELECT cumulative_sum(value) FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  cnosql.Unsigned,
			itrs: []query.Iterator{
				&UnsignedIterator{Points: []query.UnsignedPoint{
					{Name: "air", Time: 0 * Second, Value: 20},
					{Name: "air", Time: 0 * Second, Value: 19},
					{Name: "air", Time: 4 * Second, Value: 10},
					{Name: "air", Time: 4 * Second, Value: 3},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(39)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(49)}},
				{Time: 4 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(52)}},
			},
		},
		{
			name: "HoltWinters_GroupBy_Agg",
			q:    `SELECT holt_winters(mean(value), 2, 2) FROM air WHERE time >= '1970-01-01T00:00:10Z' AND time < '1970-01-01T00:00:20Z' GROUP BY time(2s)`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Time: 10 * Second, Value: 4},
					{Name: "air", Time: 11 * Second, Value: 6},

					{Name: "air", Time: 12 * Second, Value: 9},
					{Name: "air", Time: 13 * Second, Value: 11},

					{Name: "air", Time: 14 * Second, Value: 5},
					{Name: "air", Time: 15 * Second, Value: 7},

					{Name: "air", Time: 16 * Second, Value: 10},
					{Name: "air", Time: 17 * Second, Value: 12},

					{Name: "air", Time: 18 * Second, Value: 6},
					{Name: "air", Time: 19 * Second, Value: 8},
				}},
			},
			rows: []query.Row{
				{Time: 20 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{11.960623419918432}},
				{Time: 22 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{7.953140268154609}},
			},
		},
		{
			name: "DuplicateSelectors",
			q:    `SELECT min(value) * 2, min(value) / 2 FROM air WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), station fill(none)`,
			typ:  cnosql.Float,
			expr: `min(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 0 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 11 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 31 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 9 * Second, Value: 19},
					{Name: "air", Tags: ParseTags("region=east,station=A"), Time: 10 * Second, Value: 2},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 5 * Second, Value: 10},
				}},
			},
			rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(38), float64(19) / 2}},
				{Time: 10 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(4), float64(1)}},
				{Time: 30 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=A")}, Values: []interface{}{float64(200), float64(50)}},
				{Time: 0 * Second, Series: query.Series{Name: "air", Tags: ParseTags("station=B")}, Values: []interface{}{float64(20), float64(5)}},
			},
		},
		{
			name: "GroupByOffset",
			q:    `SELECT mean(value) FROM air WHERE time >= now() - 2m AND time < now() GROUP BY time(1m, now())`,
			typ:  cnosql.Float,
			expr: `mean(value::float)`,
			itrs: []query.Iterator{
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 34 * Second, Value: 20},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 57 * Second, Value: 3},
					{Name: "air", Tags: ParseTags("region=west,station=A"), Time: 92 * Second, Value: 100},
				}},
				&FloatIterator{Points: []query.FloatPoint{
					{Name: "air", Tags: ParseTags("region=west,station=B"), Time: 45 * Second, Value: 10},
				}},
			},
			rows: []query.Row{
				{Time: 30 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(11)}},
				{Time: 90 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
			},
			now: mustParseTime("1970-01-01T00:02:30Z"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shardMapper := ShardMapper{
				MapShardsFn: func(sources cnosql.Sources, _ cnosql.TimeRange) query.ShardGroup {
					var fields map[string]cnosql.DataType
					if tt.typ != cnosql.Unknown {
						fields = map[string]cnosql.DataType{"value": tt.typ}
					} else {
						fields = tt.fields
					}
					return &ShardGroup{
						Fields:     fields,
						Dimensions: []string{"station", "region"},
						CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
							if m.Name != "air" {
								t.Fatalf("unexpected source: %s", m.Name)
							}
							if tt.expr != "" && !reflect.DeepEqual(opt.Expr, MustParseExpr(tt.expr)) {
								t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
							}

							itrs := tt.itrs
							if _, ok := opt.Expr.(*cnosql.Call); ok {
								for i, itr := range itrs {
									itr, err := query.NewCallIterator(itr, opt)
									if err != nil {
										return nil, err
									}
									itrs[i] = itr
								}
							}
							return query.Iterators(itrs).Merge(opt)
						},
					}
				},
			}

			stmt := MustParseSelectStatement(tt.q)
			stmt.OmitTime = true
			cur, err := func(stmt *cnosql.SelectStatement) (query.Cursor, error) {
				c, err := query.Compile(stmt, query.CompileOptions{
					Now: tt.now,
				})
				if err != nil {
					return nil, err
				}

				p, err := c.Prepare(&shardMapper, query.SelectOptions{})
				if err != nil {
					return nil, err
				}
				return p.Select(context.Background())
			}(stmt)
			if err != nil {
				if tt.err == "" {
					t.Fatal(err)
				} else if have, want := err.Error(), tt.err; have != want {
					t.Fatalf("unexpected error: have=%s want=%s", have, want)
				}
			} else if tt.err != "" {
				t.Fatal("expected error")
			} else if a, err := ReadCursor(cur); err != nil {
				t.Fatalf("unexpected point: %s", err)
			} else if diff := cmp.Diff(tt.rows, a); diff != "" {
				t.Fatalf("unexpected points:\n%s", diff)
			}
		})
	}
}

// Ensure a SELECT with raw fields works for all types.
func TestSelect_Raw(t *testing.T) {
	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, _ cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"f": cnosql.Float,
					"i": cnosql.Integer,
					"u": cnosql.Unsigned,
					"s": cnosql.String,
					"b": cnosql.Boolean,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if m.Name != "air" {
						t.Fatalf("unexpected source: %s", m.Name)
					}
					if !reflect.DeepEqual(opt.Aux, []cnosql.VarRef{
						{Val: "b", Type: cnosql.Boolean},
						{Val: "f", Type: cnosql.Float},
						{Val: "i", Type: cnosql.Integer},
						{Val: "s", Type: cnosql.String},
						{Val: "u", Type: cnosql.Unsigned},
					}) {
						t.Fatalf("unexpected auxiliary fields: %v", opt.Aux)
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "air", Time: 0 * Second, Aux: []interface{}{
							true, float64(20), int64(20), "a", uint64(20)}},
						{Name: "air", Time: 5 * Second, Aux: []interface{}{
							false, float64(10), int64(10), "b", uint64(10)}},
						{Name: "air", Time: 9 * Second, Aux: []interface{}{
							true, float64(19), int64(19), "c", uint64(19)}},
					}}, nil
				},
			}
		},
	}

	stmt := MustParseSelectStatement(`SELECT f, i, u, s, b FROM air`)
	stmt.OmitTime = true
	cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{})
	if err != nil {
		t.Errorf("parse error: %s", err)
	} else if a, err := ReadCursor(cur); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff([]query.Row{
		{
			Time: 0 * Second,
			Series: query.Series{
				Name: "air",
			},
			Values: []interface{}{float64(20), int64(20), uint64(20), "a", true},
		},
		{
			Time: 5 * Second,
			Series: query.Series{
				Name: "air",
			},
			Values: []interface{}{float64(10), int64(10), uint64(10), "b", false},
		},
		{
			Time: 9 * Second,
			Series: query.Series{
				Name: "air",
			},
			Values: []interface{}{float64(19), int64(19), uint64(19), "c", true},
		},
	}, a); diff != "" {
		t.Errorf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT binary expr queries can be executed as floats.
func TestSelect_BinaryExpr(t *testing.T) {
	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, _ cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"f": cnosql.Float,
					"i": cnosql.Integer,
					"u": cnosql.Unsigned,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if m.Name != "air" {
						t.Fatalf("unexpected source: %s", m.Name)
					}
					makeAuxFields := func(value int) []interface{} {
						aux := make([]interface{}, len(opt.Aux))
						for i := range aux {
							switch opt.Aux[i].Type {
							case cnosql.Float:
								aux[i] = float64(value)
							case cnosql.Integer:
								aux[i] = int64(value)
							case cnosql.Unsigned:
								aux[i] = uint64(value)
							}
						}
						return aux
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "air", Time: 0 * Second, Aux: makeAuxFields(20)},
						{Name: "air", Time: 5 * Second, Aux: makeAuxFields(10)},
						{Name: "air", Time: 9 * Second, Aux: makeAuxFields(19)},
					}}, nil
				},
			}
		},
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Rows      []query.Row
		Err       string
	}{
		{
			Name:      "Float_AdditionRHS_Number",
			Statement: `SELECT f + 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Integer_AdditionRHS_Number",
			Statement: `SELECT i + 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Unsigned_AdditionRHS_Number",
			Statement: `SELECT u + 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Float_AdditionRHS_Integer",
			Statement: `SELECT f + 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Integer_AdditionRHS_Integer",
			Statement: `SELECT i + 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(21)}},
			},
		},
		{
			Name:      "Unsigned_AdditionRHS_Integer",
			Statement: `SELECT u + 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(21)}},
			},
		},
		{
			Name:      "Float_AdditionRHS_Unsigned",
			Statement: `SELECT f + 9223372036854775808 FROM air`,
			Rows: []query.Row{ // adding small floats to this does not change the value, this is expected
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
			},
		},
		{
			Name:      "Integer_AdditionRHS_Unsigned",
			Statement: `SELECT i + 9223372036854775808 FROM air`,
			Err:       `type error: i::integer + 9223372036854775808: cannot use + with an integer and unsigned literal`,
		},
		{
			Name:      "Unsigned_AdditionRHS_Unsigned",
			Statement: `SELECT u + 9223372036854775808 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775828)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775818)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775827)}},
			},
		},
		{
			Name:      "Float_AdditionLHS_Number",
			Statement: `SELECT 2.0 + f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Integer_AdditionLHS_Number",
			Statement: `SELECT 2.0 + i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Unsigned_AdditionLHS_Number",
			Statement: `SELECT 2.0 + u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Float_AdditionLHS_Integer",
			Statement: `SELECT 2 + f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(21)}},
			},
		},
		{
			Name:      "Integer_AdditionLHS_Integer",
			Statement: `SELECT 2 + i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(21)}},
			},
		},
		{
			Name:      "Unsigned_AdditionLHS_Integer",
			Statement: `SELECT 2 + u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(22)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(12)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(21)}},
			},
		},
		{
			Name:      "Float_AdditionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 + f FROM air`,
			Rows: []query.Row{ // adding small floats to this does not change the value, this is expected
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775808)}},
			},
		},
		{
			Name:      "Integer_AdditionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 + i FROM air`,
			Err:       `type error: 9223372036854775808 + i::integer: cannot use + with an integer and unsigned literal`,
		},
		{
			Name:      "Unsigned_AdditionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 + u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775828)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775818)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775827)}},
			},
		},
		{
			Name:      "Float_Add_Float",
			Statement: `SELECT f + f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_Add_Integer",
			Statement: `SELECT i + i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(38)}},
			},
		},
		{
			Name:      "Unsigned_Add_Unsigned",
			Statement: `SELECT u + u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(38)}},
			},
		},
		{
			Name:      "Float_Add_Integer",
			Statement: `SELECT f + i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Float_Add_Unsigned",
			Statement: `SELECT f + u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_Add_Unsigned",
			Statement: `SELECT i + u FROM air`,
			Err:       `type error: i::integer + u::unsigned: cannot use + between an integer and unsigned, an explicit cast is required`,
		},
		{
			Name:      "Float_MultiplicationRHS_Number",
			Statement: `SELECT f * 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_MultiplicationRHS_Number",
			Statement: `SELECT i * 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Unsigned_MultiplicationRHS_Number",
			Statement: `SELECT u * 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Float_MultiplicationRHS_Integer",
			Statement: `SELECT f * 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_MultiplicationRHS_Integer",
			Statement: `SELECT i * 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(38)}},
			},
		},
		{
			Name:      "Unsigned_MultiplicationRHS_Integer",
			Statement: `SELECT u * 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(38)}},
			},
		},
		// Skip unsigned literals for multiplication because there is inevitable
		// overflow. While it is possible to do, the behavior is considered undefined
		// and it's not a very good test because it would result in just plugging
		// the values into the computer anyway to figure out what the correct answer
		// is rather than calculating it myself and testing that I get the correct
		// value.
		{
			Name:      "Float_MultiplicationLHS_Number",
			Statement: `SELECT 2.0 * f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_MultiplicationLHS_Number",
			Statement: `SELECT 2.0 * i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Unsigned_MultiplicationLHS_Number",
			Statement: `SELECT 2.0 * u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Float_MultiplicationLHS_Integer",
			Statement: `SELECT 2 * f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(38)}},
			},
		},
		{
			Name:      "Integer_MultiplicationLHS_Integer",
			Statement: `SELECT 2 * i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(38)}},
			},
		},
		{
			Name:      "Unsigned_MultiplicationLHS_Integer",
			Statement: `SELECT 2 * u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(40)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(38)}},
			},
		},
		// Skip unsigned literals for multiplication. See above.
		{
			Name:      "Float_Multiply_Float",
			Statement: `SELECT f * f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(400)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(361)}},
			},
		},
		{
			Name:      "Integer_Multiply_Integer",
			Statement: `SELECT i * i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(400)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(100)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(361)}},
			},
		},
		{
			Name:      "Unsigned_Multiply_Unsigned",
			Statement: `SELECT u * u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(400)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(100)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(361)}},
			},
		},
		{
			Name:      "Float_Multiply_Integer",
			Statement: `SELECT f * i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(400)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(361)}},
			},
		},
		{
			Name:      "Float_Multiply_Unsigned",
			Statement: `SELECT f * u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(400)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(100)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(361)}},
			},
		},
		{
			Name:      "Integer_Multiply_Unsigned",
			Statement: `SELECT i * u FROM air`,
			Err:       `type error: i::integer * u::unsigned: cannot use * between an integer and unsigned, an explicit cast is required`,
		},
		{
			Name:      "Float_SubtractionRHS_Number",
			Statement: `SELECT f - 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(17)}},
			},
		},
		{
			Name:      "Integer_SubtractionRHS_Number",
			Statement: `SELECT i - 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(17)}},
			},
		},
		{
			Name:      "Unsigned_SubtractionRHS_Number",
			Statement: `SELECT u - 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(17)}},
			},
		},
		{
			Name:      "Float_SubtractionRHS_Integer",
			Statement: `SELECT f - 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(17)}},
			},
		},
		{
			Name:      "Integer_SubtractionRHS_Integer",
			Statement: `SELECT i - 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(17)}},
			},
		},
		{
			Name:      "Unsigned_SubtractionRHS_Integer",
			Statement: `SELECT u - 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(17)}},
			},
		},
		{
			Name:      "Float_SubtractionRHS_Unsigned",
			Statement: `SELECT f - 9223372036854775808 FROM air`,
			Rows: []query.Row{ // adding small floats to this does not change the value, this is expected
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-9223372036854775808)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-9223372036854775808)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-9223372036854775808)}},
			},
		},
		{
			Name:      "Integer_SubtractionRHS_Unsigned",
			Statement: `SELECT i - 9223372036854775808 FROM air`,
			Err:       `type error: i::integer - 9223372036854775808: cannot use - with an integer and unsigned literal`,
		},
		// Skip Unsigned_SubtractionRHS_Integer because it would result in underflow.
		{
			Name:      "Float_SubtractionLHS_Number",
			Statement: `SELECT 2.0 - f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-17)}},
			},
		},
		{
			Name:      "Integer_SubtractionLHS_Number",
			Statement: `SELECT 2.0 - i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-17)}},
			},
		},
		{
			Name:      "Unsigned_SubtractionLHS_Number",
			Statement: `SELECT 2.0 - u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-17)}},
			},
		},
		{
			Name:      "Float_SubtractionLHS_Integer",
			Statement: `SELECT 2 - f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-17)}},
			},
		},
		{
			Name:      "Integer_SubtractionLHS_Integer",
			Statement: `SELECT 2 - i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-18)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(-17)}},
			},
		},
		{
			Name:      "Unsigned_SubtractionLHS_Integer",
			Statement: `SELECT 30 - u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(11)}},
			},
		},
		{
			Name:      "Float_SubtractionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 - f FROM air`, // subtracting small floats to this does not change the value, this is expected
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775828)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775828)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(9223372036854775828)}},
			},
		},
		{
			Name:      "Integer_SubtractionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 - i FROM air`,
			Err:       `type error: 9223372036854775808 - i::integer: cannot use - with an integer and unsigned literal`,
		},
		{
			Name:      "Unsigned_SubtractionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 - u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775788)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775798)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9223372036854775789)}},
			},
		},
		{
			Name:      "Float_Subtract_Float",
			Statement: `SELECT f - f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
			},
		},
		{
			Name:      "Integer_Subtract_Integer",
			Statement: `SELECT i - i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
			},
		},
		{
			Name:      "Unsigned_Subtract_Unsigned",
			Statement: `SELECT u - u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
			},
		},
		{
			Name:      "Float_Subtract_Integer",
			Statement: `SELECT f - i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
			},
		},
		{
			Name:      "Float_Subtract_Unsigned",
			Statement: `SELECT f - u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(0)}},
			},
		},
		{
			Name:      "Integer_Subtract_Unsigned",
			Statement: `SELECT i - u FROM air`,
			Err:       `type error: i::integer - u::unsigned: cannot use - between an integer and unsigned, an explicit cast is required`,
		},
		{
			Name:      "Float_DivisionRHS_Number",
			Statement: `SELECT f / 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / 2}},
			},
		},
		{
			Name:      "Integer_DivisionRHS_Number",
			Statement: `SELECT i / 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / 2}},
			},
		},
		{
			Name:      "Unsigned_DivisionRHS_Number",
			Statement: `SELECT u / 2.0 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / 2}},
			},
		},
		{
			Name:      "Float_DivisionRHS_Integer",
			Statement: `SELECT f / 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / 2}},
			},
		},
		{
			Name:      "Integer_DivisionRHS_Integer",
			Statement: `SELECT i / 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / 2}},
			},
		},
		{
			Name:      "Unsigned_DivisionRHS_Integer",
			Statement: `SELECT u / 2 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(10)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(9)}},
			},
		},
		{
			Name:      "Float_DivisionRHS_Unsigned",
			Statement: `SELECT f / 9223372036854775808 FROM air`,
			Rows: []query.Row{ // dividing small floats does not result in a meaningful result, this is expected
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(20) / float64(9223372036854775808)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10) / float64(9223372036854775808)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(19) / float64(9223372036854775808)}},
			},
		},
		{
			Name:      "Integer_DivisionRHS_Unsigned",
			Statement: `SELECT i / 9223372036854775808 FROM air`,
			Err:       `type error: i::integer / 9223372036854775808: cannot use / with an integer and unsigned literal`,
		},
		{
			Name:      "Unsigned_DivisionRHS_Unsigned",
			Statement: `SELECT u / 9223372036854775808 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
			},
		},
		{
			Name:      "Float_DivisionLHS_Number",
			Statement: `SELECT 38.0 / f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1.9)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3.8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2)}},
			},
		},
		{
			Name:      "Integer_DivisionLHS_Number",
			Statement: `SELECT 38.0 / i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1.9)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3.8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2)}},
			},
		},
		{
			Name:      "Unsigned_DivisionLHS_Number",
			Statement: `SELECT 38.0 / u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1.9)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3.8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2)}},
			},
		},
		{
			Name:      "Float_DivisionLHS_Integer",
			Statement: `SELECT 38 / f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1.9)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3.8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2)}},
			},
		},
		{
			Name:      "Integer_DivisionLHS_Integer",
			Statement: `SELECT 38 / i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1.9)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(3.8)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(2)}},
			},
		},
		{
			Name:      "Unsigned_DivisionLHS_Integer",
			Statement: `SELECT 38 / u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(3)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(2)}},
			},
		},
		{
			Name:      "Float_DivisionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 / f FROM air`,
			Rows: []query.Row{ // dividing large floats results in inaccurate outputs so these may not be correct, but that is considered normal for floating point
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(461168601842738816)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(922337203685477632)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(485440633518672384)}},
			},
		},
		{
			Name:      "Integer_DivisionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 / i FROM air`,
			Err:       `type error: 9223372036854775808 / i::integer: cannot use / with an integer and unsigned literal`,
		},
		{
			Name:      "Unsigned_DivisionLHS_Unsigned",
			Statement: `SELECT 9223372036854775808 / u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(461168601842738790)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(922337203685477580)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(485440633518672410)}},
			},
		},
		{
			Name:      "Float_Divide_Float",
			Statement: `SELECT f / f FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
			},
		},
		{
			Name:      "Integer_Divide_Integer",
			Statement: `SELECT i / i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
			},
		},
		{
			Name:      "Unsigned_Divide_Unsigned",
			Statement: `SELECT u / u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(1)}},
			},
		},
		{
			Name:      "Float_Divide_Integer",
			Statement: `SELECT f / i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
			},
		},
		{
			Name:      "Float_Divide_Unsigned",
			Statement: `SELECT f / u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(1)}},
			},
		},
		{
			Name:      "Integer_Divide_Unsigned",
			Statement: `SELECT i / u FROM air`,
			Err:       `type error: i::integer / u::unsigned: cannot use / between an integer and unsigned, an explicit cast is required`,
		},
		{
			Name:      "Integer_BitwiseAndRHS",
			Statement: `SELECT i & 254 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(10)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(18)}},
			},
		},
		{
			Name:      "Unsigned_BitwiseAndRHS",
			Statement: `SELECT u & 254 FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(10)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(18)}},
			},
		},
		{
			Name:      "Integer_BitwiseOrLHS",
			Statement: `SELECT 4 | i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(14)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(23)}},
			},
		},
		{
			Name:      "Unsigned_BitwiseOrLHS",
			Statement: `SELECT 4 | u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(20)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(14)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(23)}},
			},
		},
		{
			Name:      "Integer_BitwiseXOr_Integer",
			Statement: `SELECT i ^ i FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{int64(0)}},
			},
		},
		{
			Name:      "Unsigned_BitwiseXOr_Integer",
			Statement: `SELECT u ^ u FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{uint64(0)}},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			stmt := MustParseSelectStatement(test.Statement)
			stmt.OmitTime = true
			cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{})
			if err != nil {
				if have, want := err.Error(), test.Err; want != "" {
					if have != want {
						t.Errorf("%s: unexpected parse error: %s != %s", test.Name, have, want)
					}
				} else {
					t.Errorf("%s: unexpected parse error: %s", test.Name, have)
				}
			} else if test.Err != "" {
				t.Fatalf("%s: expected error", test.Name)
			} else if a, err := ReadCursor(cur); err != nil {
				t.Fatalf("%s: unexpected error: %s", test.Name, err)
			} else if diff := cmp.Diff(test.Rows, a); diff != "" {
				t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
			}
		})
	}
}

// Ensure a SELECT binary expr queries can be executed as booleans.
func TestSelect_BinaryExpr_Boolean(t *testing.T) {
	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, _ cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"one": cnosql.Boolean,
					"two": cnosql.Boolean,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if m.Name != "air" {
						t.Fatalf("unexpected source: %s", m.Name)
					}
					makeAuxFields := func(value bool) []interface{} {
						aux := make([]interface{}, len(opt.Aux))
						for i := range aux {
							aux[i] = value
						}
						return aux
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "air", Time: 0 * Second, Aux: makeAuxFields(true)},
						{Name: "air", Time: 5 * Second, Aux: makeAuxFields(false)},
						{Name: "air", Time: 9 * Second, Aux: makeAuxFields(true)},
					}}, nil
				},
			}
		},
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Rows      []query.Row
	}{
		{
			Name:      "BinaryXOrRHS",
			Statement: `SELECT one ^ true FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{false}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{false}},
			},
		},
		{
			Name:      "BinaryOrLHS",
			Statement: `SELECT true | two FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
			},
		},
		{
			Name:      "TwoSeriesBitwiseAnd",
			Statement: `SELECT one & two FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{false}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{true}},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			stmt := MustParseSelectStatement(test.Statement)
			stmt.OmitTime = true
			cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{})
			if err != nil {
				t.Errorf("%s: parse error: %s", test.Name, err)
			} else if a, err := ReadCursor(cur); err != nil {
				t.Fatalf("%s: unexpected error: %s", test.Name, err)
			} else if diff := cmp.Diff(test.Rows, a); diff != "" {
				t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
			}
		})
	}
}

// Ensure a SELECT binary expr with nil values can be executed.
// Nil values may be present when a field is missing from one iterator,
// but not the other.
func TestSelect_BinaryExpr_NilValues(t *testing.T) {
	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, _ cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"total": cnosql.Float,
					"value": cnosql.Float,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if m.Name != "air" {
						t.Fatalf("unexpected source: %s", m.Name)
					}
					return &FloatIterator{Points: []query.FloatPoint{
						{Name: "air", Time: 0 * Second, Aux: []interface{}{float64(20), nil}},
						{Name: "air", Time: 5 * Second, Aux: []interface{}{float64(10), float64(15)}},
						{Name: "air", Time: 9 * Second, Aux: []interface{}{nil, float64(5)}},
					}}, nil
				},
			}
		},
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Rows      []query.Row
	}{
		{
			Name:      "Addition",
			Statement: `SELECT total + value FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(25)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
			},
		},
		{
			Name:      "Subtraction",
			Statement: `SELECT total - value FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(-5)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
			},
		},
		{
			Name:      "Multiplication",
			Statement: `SELECT total * value FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(150)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
			},
		},
		{
			Name:      "Division",
			Statement: `SELECT total / value FROM air`,
			Rows: []query.Row{
				{Time: 0 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
				{Time: 5 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{float64(10) / float64(15)}},
				{Time: 9 * Second, Series: query.Series{Name: "air"}, Values: []interface{}{nil}},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			stmt := MustParseSelectStatement(test.Statement)
			stmt.OmitTime = true
			cur, err := query.Select(context.Background(), stmt, &shardMapper, query.SelectOptions{})
			if err != nil {
				t.Errorf("%s: parse error: %s", test.Name, err)
			} else if a, err := ReadCursor(cur); err != nil {
				t.Fatalf("%s: unexpected error: %s", test.Name, err)
			} else if diff := cmp.Diff(test.Rows, a); diff != "" {
				t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
			}
		})
	}
}

// ReadCursor reads a Cursor into an array of points.
func ReadCursor(cur query.Cursor) ([]query.Row, error) {
	defer cur.Close()

	var rows []query.Row
	for {
		var row query.Row
		if !cur.Scan(&row) {
			if err := cur.Err(); err != nil {
				return nil, err
			}
			return rows, nil
		}
		rows = append(rows, row)
	}
}
