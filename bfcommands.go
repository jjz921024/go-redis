package redis

import "context"

func (c cmdable) BfReserve(ctx context.Context, key string, errorRate float64, capacity int) *StatusCmd {
	args := []interface{}{"bf_reserve", key, errorRate, capacity, "NONSCALING"}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfAdd(ctx context.Context, key string, element interface{}) *IntCmd {
	args := []interface{}{"bf_add", key, element}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfMAdd(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"bf_madd", key}
	args = append(args, elements...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfInsert(ctx context.Context, key string, errorRate float64, capacity int, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"bf_insert", key}
	args = append(args, elements...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfExists(ctx context.Context, key string, element interface{}) *IntCmd {
	args := []interface{}{"bf_exists", key, element}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfMExists(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"bf_mexists", key}
	args = append(args, elements...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfDel(ctx context.Context, key string) *IntCmd {
	args := []interface{}{"bf_del", key}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BfInfo(ctx context.Context, key string) *SliceCmd {
	args := []interface{}{"bf_info", key}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
