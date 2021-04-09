// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

type Limit float64

// demo
/*
cnt := 0
limiter := rate.NewLimiter(0, 0)
for {

	if err := limiter.Wait(context.Background()); err != nil {
		fmt.Println("err: ", err)
		continue
	}
	fmt.Println("cnt: ", cnt)
	cnt++
}
*/

// 主要分析NewLimiter|Wait实现  其他次要
// Wait 令牌不够时等待
// Allow 令牌不够时放过 可用于丢弃请求

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
// 最大数值(+∞)，令牌桶设置的每秒补充令牌数配置是Inf视为不限流
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

type Limiter struct {
	mu     sync.Mutex
	limit  Limit   // 限流数，如果是10 那么每秒恢复10个，如果是Inf每秒恢复无穷个视为不限流
	burst  int     // 最大爆桶数，如果是10，那么同时获取第11个的时候爆桶
	tokens float64 //桶内当前令牌数
	// last is the last time the limiter's tokens field was updated
	last time.Time
	// lastEvent is the latest time of a rate-limited event (past or future)
	lastEvent time.Time
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

func (lim *Limiter) Burst() int {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.burst
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
// 创建令牌桶 限流器
func NewLimiter(r Limit, b int) *Limiter {
	return &Limiter{
		limit: r,
		burst: b,
	}
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
func (lim *Limiter) AllowN(now time.Time, n int) bool {
	return lim.reserveN(now, n, 0).ok
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok        bool
	lim       *Limiter
	tokens    int
	timeToAct time.Time
	// This is the Limit at reservation time, it can change later.
	limit Limit
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// 无穷大(+∞)时间间隔
const InfDuration = time.Duration(1<<63 - 1)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the limiter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
	return
}

func (r *Reservation) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	if r.lim.limit == Inf || r.tokens == 0 || r.timeToAct.Before(now) {
		return
	}

	// calculate tokens to restore
	// The duration between lim.lastEvent and r.timeToAct tells us how many tokens were reserved
	// after r was obtained. These tokens should not be restored.
	restoreTokens := float64(r.tokens) - r.limit.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct))
	if restoreTokens <= 0 {
		return
	}
	// advance time to now
	now, _, tokens := r.lim.advance(now)
	// calculate new number of tokens
	tokens += restoreTokens
	if burst := float64(r.lim.burst); tokens > burst {
		tokens = burst
	}
	// update state
	r.lim.last = now
	r.lim.tokens = tokens
	if r.timeToAct == r.lim.lastEvent {
		prevEvent := r.timeToAct.Add(r.limit.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(now) {
			r.lim.lastEvent = prevEvent
		}
	}

	return
}

func (lim *Limiter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

func (lim *Limiter) ReserveN(now time.Time, n int) *Reservation {
	r := lim.reserveN(now, n, InfDuration)
	return &r
}

// Wait is shorthand for WaitN(ctx, 1).
// 获取令牌接口
func (lim *Limiter) Wait(ctx context.Context) (err error) {
	return lim.WaitN(ctx, 1) // 调用获取N个令牌的接口
}

func (lim *Limiter) WaitN(ctx context.Context, n int) (err error) {
	lim.mu.Lock()
	burst := lim.burst
	limit := lim.limit
	lim.mu.Unlock()

	// limit == Inf 是不限流模式
	// 所以需要的令牌数n大于最大令牌数 并且 在限流模式下 直接返回错误
	if n > burst && limit != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's burst %d", n, burst)
	}

	// 检查上下文是不是背取消或者超时，如果取消或者超时直接返回错误
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// ctx没有取消或者超时 继续执行
	now := time.Now()
	waitLimit := InfDuration // 可以等待的时间间隔，如果ctx是不设置超时的那么可以等待的时间间隔是无穷大
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now) //可以等待的时间间隔，如果ctx是超时的，那么可以等待的时间是ddl和当前时间的差值
	}
	// Reserve
	r := lim.reserveN(now, n, waitLimit) // 申请N个令牌
	if !r.ok {
		//申请失败直接返回错误
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// 申请成功继续执行

	// 计算出来的执行时间 和 当前时间做差值,计算出执行的delay时间
	delay := r.DelayFrom(now)
	if delay == 0 { // 无需等待 直接执行
		return nil
	}

	t := time.NewTimer(delay) // 生成定时器
	defer t.Stop()
	select {
	case <-t.C: // 需要等待的时间，这里是等待不够的令牌数 需要补充的时间
		// We can proceed.
		return nil
	case <-ctx.Done(): // ctx超时 返回错误哦
		r.Cancel()
		return ctx.Err()
	}
}

// SetLimit is shorthand for SetLimitAt(time.Now(), newLimit).
func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.SetLimitAt(time.Now(), newLimit)
}

// SetLimitAt sets a new Limit for the limiter. The new Limit, and Burst, may be violated
// or underutilized by those which reserved (using Reserve or Wait) but did not yet act
// before SetLimitAt was called.
func (lim *Limiter) SetLimitAt(now time.Time, newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	now, _, tokens := lim.advance(now)

	lim.last = now
	lim.tokens = tokens
	lim.limit = newLimit
}

// SetBurst is shorthand for SetBurstAt(time.Now(), newBurst).
func (lim *Limiter) SetBurst(newBurst int) {
	lim.SetBurstAt(time.Now(), newBurst)
}

// SetBurstAt sets a new burst size for the limiter.
func (lim *Limiter) SetBurstAt(now time.Time, newBurst int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	now, _, tokens := lim.advance(now)

	lim.last = now
	lim.tokens = tokens
	lim.burst = newBurst
}

// reserveN is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
func (lim *Limiter) reserveN(now time.Time, n int, maxFutureReserve time.Duration) Reservation {
	lim.mu.Lock() // 上锁

	// 不限流解锁返回
	if lim.limit == Inf {
		lim.mu.Unlock()
		return Reservation{
			ok:        true,
			lim:       lim,
			tokens:    n,
			timeToAct: now,
		}
	}

	// 补充令牌并且返回当前令牌桶信息
	now, last, tokens := lim.advance(now)

	// 申请了n个令牌，当前令牌数设置成tokens-n
	tokens -= float64(n)

	// 计算等待时间
	var waitDuration time.Duration
	if tokens < 0 { //如果当前令牌数不够了，等补充吧
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}

	// 申请的令牌数n小于令牌桶可容纳的令牌数 并且 在ctx设置的超时时间内 可分配
	// 否则不可分配
	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:    ok,
		lim:   lim,
		limit: lim.limit,
	}
	if ok {
		r.tokens = n
		r.timeToAct = now.Add(waitDuration) // 什么时候触发执行
	}

	// Update state
	if ok {
		lim.last = now
		lim.tokens = tokens
		lim.lastEvent = r.timeToAct
	} else {
		lim.last = last
	}

	lim.mu.Unlock()
	return r
}

// 补充令牌并且返回当前令牌桶信息
func (lim *Limiter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}

	// durationFromTokens    	令牌数 转换成时间的接口
	// tokensFromDuration		时间 转换成令牌数的接口
	// 这里时间和令牌数是对等的，但是令牌桶算法不是严格匀速算法，可以秒内爆发 即同一时间点 把所有令牌申请走，不控制申请速度

	// 计算最大可补充的令牌数 对应的时间
	maxElapsed := lim.limit.durationFromTokens(float64(lim.burst) - lim.tokens)

	// 用上一次补充的时间 和当前时间的差值做为 可补充令牌数 对应的事件
	elapsed := now.Sub(last)

	// 可能存在补充间隔太长出现补充溢出的问题，和最大可补充时间调整下
	if elapsed > maxElapsed {
		elapsed = maxElapsed
	}

	// 可补充时间转换为 可补充令牌数
	delta := lim.limit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta

	// 当前令牌数大于最大限制令牌数 调整当前为最大即可
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}

	return now, last, tokens
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	seconds := tokens / float64(limit)

	// 按纳秒为单位 分配令牌
	return time.Nanosecond * time.Duration(1e9*seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	// Split the integer and fractional parts ourself to minimize rounding errors.
	// See golang.org/issues/34861.
	sec := float64(d/time.Second) * float64(limit)
	nsec := float64(d%time.Second) * float64(limit)
	return sec + nsec/1e9
}
