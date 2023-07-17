package redis

import (
	"context"
	"time"
)

type ExSetArgs struct {
	SetArgs

	Ver int64
	Abs int64
}

func (a *SetArgs) getArgs() []interface{} {
	args := make([]interface{}, 0)

	if a.KeepTTL {
		args = append(args, "keepttl")
	}

	if !a.ExpireAt.IsZero() {
		args = append(args, "exat", a.ExpireAt.Unix())
	}
	if a.TTL > 0 {
		if usePrecise(a.TTL) {
			args = append(args, "px", formatMs(context.TODO(), a.TTL))
		} else {
			args = append(args, "ex", formatSec(context.TODO(), a.TTL))
		}
	}

	if a.Mode != "" {
		args = append(args, a.Mode)
	}

	return args
}

func (a *ExSetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	args = append(args, a.getArgs()...)

	if a.Ver != 0 {
		args = append(args, "ver", a.Ver)
	}
	if a.Abs != 0 {
		args = append(args, "abs", a.Abs)
	}

	return args
}

type ExIncrByArgs struct {
	SetArgs

	Ver int64
	Abs int64

	Min int64
	Max int64

	NoNegative bool
}

func (a *ExIncrByArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	args = append(args, a.getArgs()...)

	if a.Ver != 0 {
		args = append(args, "ver", a.Ver)
	}
	if a.Abs != 0 {
		args = append(args, "abs", a.Abs)
	}

	if a.Min != 0 {
		args = append(args, "min", a.Min)
	}
	if a.Max != 0 {
		args = append(args, "max", a.Max)
	}

	if a.NoNegative {
		args = append(args, "nonegative")
	}

	return args
}

type ExHIncrArgs struct {
	ExIncrByArgs

	Gt      int64
}

func (a *ExHIncrArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	args = append(args, a.getArgs()...)

	if a.Gt != 0 {
		args = append(args, "gt", a.Gt)
	}

	return args
}


// TairString Commands

func (c cmdable) Cas(ctx context.Context, key string, oldVal, newVal interface{}) *IntCmd {
	args := []interface{}{"cas", key, oldVal, newVal}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CasArgs(ctx context.Context, key string, oldVal, newVal interface{}, a *SetArgs) *IntCmd {
	args := []interface{}{"cas", key, oldVal, newVal}

	if !a.ExpireAt.IsZero() {
		args = append(args, "exat", a.ExpireAt.Unix())
	}
	if a.TTL > 0 {
		if usePrecise(a.TTL) {
			args = append(args, "px", formatMs(ctx, a.TTL))
		} else {
			args = append(args, "ex", formatSec(ctx, a.TTL))
		}
	}

	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Cad(ctx context.Context, key string, value interface{}) *IntCmd {
	args := []interface{}{"cad", key, value}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExSet(ctx context.Context, key string, value interface{}) *StatusCmd {
	args := []interface{}{"exset", key, value}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExSetArgs(ctx context.Context, key string, value interface{}, a *ExSetArgs) *StatusCmd {
	args := []interface{}{"exset", key, value}
	args = append(args, a.GetArgs()...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExSetWithVersion(ctx context.Context, key string, value interface{}, a *ExSetArgs) *IntCmd {
	args := []interface{}{"exset", key, value, "withversion"}
	args = append(args, a.GetArgs()...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExSetVer(ctx context.Context, key string, version int64) *IntCmd {
	args := []interface{}{"exset", key, version}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExGet(ctx context.Context, key string) *SliceCmd {
	cmd := NewSliceCmd(ctx, "exget", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExIncrBy(ctx context.Context, key string, incr int64) *IntCmd {
	cmd := NewIntCmd(ctx, "exincrby", key, incr)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExIncrByArgs(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *IntCmd {
	args := []interface{}{"exincrby", key, incr}
	args = append(args, a.GetArgs()...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExIncrByWithVersion(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *SliceCmd {
	args := []interface{}{"exincrby", key, incr, "withversion"}
	args = append(args, a.GetArgs()...)
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExIncrByFloat(ctx context.Context, key string, incr float64) *FloatCmd {
	args := []interface{}{"exincrbyfloat", key, incr}
	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExIncrByFloatArgs(ctx context.Context, key string, incr float64, a *ExIncrByArgs) *FloatCmd {
	args := []interface{}{"exincrbyfloat", key, incr}
	args = append(args, a.GetArgs()...)
	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExCas(ctx context.Context, key string, newVal interface{}, version int64) *SliceCmd {
	args := []interface{}{"excas", key, newVal, version}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExCad(ctx context.Context, key string, version int) *IntCmd {
	args := []interface{}{"excad", key, version}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int64) *IntCmd {
	args := []interface{}{"exappend", key, value, nxxx, verAbs, version}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExPreAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int64) *IntCmd {
	args := []interface{}{"exprepend", key, value, nxxx, verAbs, version}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExGae(ctx context.Context, key string, time time.Duration) *SliceCmd {
	args := []interface{}{"exgae", key, time}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}


// TairHash Commands

func (c cmdable) ExHSet(ctx context.Context, key, field, value string) *IntCmd {
	args := []interface{}{"exhset", key, field, value}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHSetArgs(ctx context.Context, key, field, value string, arg *ExSetArgs) *IntCmd {
	args := []interface{}{"exhset", key, field, value}
	args = append(args, arg.GetArgs()...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHSetNx(ctx context.Context, key, field, value string) *IntCmd {
	args := []interface{}{"exhsetnx", key, field, value}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHMSet(ctx context.Context, key string, fieldValue map[string]string) *StatusCmd {
	args := []interface{}{"exhmset", key}
	for k, v := range fieldValue {
		args = append(args, k)
		args = append(args, v)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHGet(ctx context.Context, key, field string) *StringCmd {
	args := []interface{}{"exhget", key, field}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHPExpire(ctx context.Context, key, field string, milliseconds int) *BoolCmd {
	args := []interface{}{"exhpexpire", key, field, milliseconds}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHPExpireAt(ctx context.Context, key, field string, unixTime int) *BoolCmd {
	args := []interface{}{"exhpexpireat", key, field, unixTime}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHExpire(ctx context.Context, key, field string, milliseconds int) *BoolCmd {
	args := []interface{}{"exhexpire", key, field, milliseconds}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHExpireAt(ctx context.Context, key, field string, unixTime int) *BoolCmd {
	args := []interface{}{"exhexpireat", key, field, unixTime}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHPTTL(ctx context.Context, key, field string) *IntCmd {
	args := []interface{}{"exhpttl", key, field}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHTTL(ctx context.Context, key, field string) *IntCmd {
	args := []interface{}{"exhttl", key, field}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHVer(ctx context.Context, key, field string) *IntCmd {
	args := []interface{}{"exhver", key, field}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHSetVer(ctx context.Context, key, field string, version int) *BoolCmd {
	args := []interface{}{"exhsetver", key, field, version}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHIncrBy(ctx context.Context, key, field string, value int) *IntCmd {
	args := []interface{}{"exhincrby", key, field, value}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHIncrByArgs(ctx context.Context, key, field string, value int, arg *ExHIncrArgs) *IntCmd {
	args := []interface{}{"exhincrby", key, field, value}
	args = append(args, arg.GetArgs()...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHIncrByFloat(ctx context.Context, key, field string, value float64) *StringCmd {
	args := []interface{}{"exhincrbyfloat", key, field, value}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHIncrByFloatArgs(ctx context.Context, key, field string, value float64, arg *ExHIncrArgs) *StringCmd {
	args := []interface{}{"exhincrbyfloat", key, field, value}
	args = append(args, arg.GetArgs()...)
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHGetWithVer(ctx context.Context, key, field string) *SliceCmd {
	args := []interface{}{"exhgetwithver", key, field}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHMGet(ctx context.Context, key string, field ...string) *StringSliceCmd {
	args := []interface{}{"exhmget", key}
	for _, f := range field {
		args = append(args, f)
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHMGetWithVer(ctx context.Context, key string, field ...string) *SliceCmd {
	args := []interface{}{"exhmgetwithver", key}
	for _, f := range field {
		args = append(args, f)
	}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHDel(ctx context.Context, key string, field ...string) *IntCmd {
	args := []interface{}{"exhdel", key}
	for _, f := range field {
		args = append(args, f)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHLen(ctx context.Context, key string) *IntCmd {
	args := []interface{}{"exhlen", key}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHExists(ctx context.Context, key, field string) *BoolCmd {
	args := []interface{}{"exhexists", key, field}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHStrLen(ctx context.Context, key, field string) *IntCmd {
	args := []interface{}{"exhstrlen", key, field}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHKeys(ctx context.Context, key string) *StringSliceCmd {
	args := []interface{}{"exhkeys", key}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHVals(ctx context.Context, key string) *StringSliceCmd {
	args := []interface{}{"exhvals", key}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHGetAll(ctx context.Context, key string) *MapStringStringCmd {
	args := []interface{}{"exhgetall", key}
	cmd := NewMapStringStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExHScan(ctx context.Context, key string, cursor string) *SliceCmd {
	args := []interface{}{"exhscan", key, cursor}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}