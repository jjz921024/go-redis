package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
)

const queryTemplate string = "http://%s/redis_observer/proxy_online_list?clusterName=%s"

type observer struct {
	cli        *http.Client
	domainUrls []string

	mu           sync.RWMutex
	currentProxy []string
}

var ob *observer
var once sync.Once

// domain: 多个ob域名, 以逗号分割, eg: 127.0.0.1:18080,127.0.0.2:18080
func getObserver(domain string, clusterName string, timeout time.Duration) *observer {
	once.Do(func() {
		cli := &http.Client{
			Timeout: timeout,
		}

		urls := []string{}
		for _, v := range strings.Split(domain, ",") {
			s := strings.TrimPrefix(v, "http://")
			urls = append(urls, fmt.Sprintf(queryTemplate, s, clusterName))
		}

		ob = &observer{
			cli:        cli,
			domainUrls: urls,
		}
	})
	return ob
}

type obResponse struct {
	Code   string      `json:"code"`
	Msg    string      `json:"msg"`
	Result []struct {
		Host string `json:"host"`
		Port int `json:"port"`
	} `json:"result"`
}


func (ob *observer) refreshCurrentProxyList() {
	obResp := &obResponse{Code: "-1"}
	for _, v := range ob.domainUrls {
		resp, err := http.Get(v)
		if err != nil {
			internal.Logger.Printf(context.TODO(), "request ob:%s err:%s\n", v, err.Error())
			continue
		}
		defer resp.Body.Close()

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			internal.Logger.Printf(context.TODO(), "read ob response body err:%s\n", err.Error())
			continue
		}

		if err := json.Unmarshal(data, obResp); err != nil {
			internal.Logger.Printf(context.TODO(), "parse ob response err:%s\n", err.Error())
			continue
		}

		if obResp.Code != "0" {
			internal.Logger.Printf(context.TODO(), "ob response fail code:%s, msg:%s\n", obResp.Code, obResp.Msg)
			continue
		}

		break
	}

	if obResp.Code != "0" {
		internal.Logger.Printf(context.TODO(), "can't refresh current proxy from ob")
		return
	}

	// 访问ob成功，更新本地缓存
	ob.mu.Lock()
	defer ob.mu.Unlock()

	ob.currentProxy = ob.currentProxy[:0]
	for _, v := range obResp.Result {
		ob.currentProxy = append(ob.currentProxy, fmt.Sprintf("%s:%d", v.Host, v.Port))
	}
}


func (ob *observer) getCurrentProxyList() []string {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.currentProxy
}

// 每分钟刷新一个当前存活的proxy列表
func (ob *observer) start() {
	interval := time.Second * 10
	time.AfterFunc(interval, func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				ob.refreshCurrentProxyList()
			}
		}()
	})

	ob.refreshCurrentProxyList()
}

func (ob *observer) close() {
	if ob.cli != nil {
		ob.cli.CloseIdleConnections()
	}
}