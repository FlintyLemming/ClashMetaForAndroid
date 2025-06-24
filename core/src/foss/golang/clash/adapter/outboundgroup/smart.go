package outboundgroup

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "path/filepath"
    "strings"
    "sync"
    "time"
    "strconv"
    "math/rand"
    "sync/atomic"

    "github.com/metacubex/mihomo/common/callback"
    "github.com/metacubex/mihomo/common/singleflight"
    "github.com/metacubex/mihomo/common/utils"
    C "github.com/metacubex/mihomo/constant"
    "github.com/metacubex/mihomo/constant/provider"
    "github.com/metacubex/mihomo/component/profile/cachefile"
    "github.com/metacubex/mihomo/component/smart"
    "github.com/metacubex/mihomo/component/smart/lightgbm"
    "github.com/metacubex/mihomo/log"
    N "github.com/metacubex/mihomo/common/net"
    "github.com/metacubex/mihomo/tunnel"
    "github.com/metacubex/mihomo/tunnel/statistic"

    "github.com/dlclark/regexp2"
)

const (
    prefetchInterval         = 10 * time.Minute
    cleanupInterval          = 180 * time.Minute
    cacheParamAdjustInterval = 5 * time.Minute
    recoveryCheckInterval    = 5 * time.Minute
    checkInterval            = 10 * time.Minute
    flushQueueInterval       = 300 * time.Second
    rankingInterval          = 60 * time.Minute
    
    failureRecovery5min      = 5 * time.Minute
    failureRecovery10min     = 10 * time.Minute
    failureRecovery15min     = 15 * time.Minute
    failureRecovery30min     = 30 * time.Minute
    
    longConnThreshold        = 10 * time.Minute
    
    networkFailureThreshold  = 5
    maxRetries               = 3

    maxCountValue            = 1000000
    maxTrafficStatValue      = 10000000.0
)

var (
    longConnProcessGroup singleflight.Group[interface{}]
    flushQueueOnce       atomic.Bool
    smartInitOnce        sync.Once
)

type smartOption func(*Smart)

type Smart struct {
	*GroupBase
	store          *smart.Store
	configName     string
    selected       string
	testUrl        string
	expectedStatus string
	disableUDP     bool
	fallback       *LoadBalance
	Hidden         bool
	Icon           string
	policyPriority []priorityRule
    ctx            context.Context
    cancel         context.CancelFunc
    wg             sync.WaitGroup
    useLightGBM      bool
    collectData      bool
    dataCollector    *lightgbm.DataCollector
    weightModel      *lightgbm.WeightModel
    strategy         string
}

type (
    priorityRule struct {
        pattern      string
        regex        *regexp2.Regexp
        factor       float64
        isRegex      bool
    }
)

func getConfigFilename() string {
	configFile := C.Path.Config()
	baseName := filepath.Base(configFile)
	filename := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	return filename
}

func NewSmart(option *GroupCommonOption, providers []provider.ProxyProvider, strategy string, options ...smartOption) (*Smart, error) {
	if strategy != "round-robin" && strategy != "sticky-sessions" {
        return nil, fmt.Errorf("%w: %s", errStrategy, strategy)
    }

    if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	lb, err := NewLoadBalance(&GroupCommonOption{
        Name:           option.Name + "-fallback",
        URL:            option.URL,
        Filter:         option.Filter,
        ExcludeFilter:  option.ExcludeFilter,
        ExcludeType:    option.ExcludeType,
        TestTimeout:    option.TestTimeout,
        MaxFailedTimes: option.MaxFailedTimes,
        DisableUDP:     option.DisableUDP,
        ExpectedStatus: option.ExpectedStatus,
        Interface:      option.Interface,
        RoutingMark:    option.RoutingMark,
    }, providers, strategy)

    if err != nil {
        return nil, err
    }

	configName := getConfigFilename()

	s := &Smart{
        GroupBase: NewGroupBase(GroupBaseOption{
            Name:           option.Name,
            Type:           C.Smart,
            Filter:         option.Filter,
            ExcludeFilter:  option.ExcludeFilter,
            ExcludeType:    option.ExcludeType,
            TestTimeout:    option.TestTimeout,
            MaxFailedTimes: option.MaxFailedTimes,
            Providers:      providers,
        }),
        testUrl:        option.URL,
        expectedStatus: option.ExpectedStatus,
        configName:     configName,
        disableUDP:     option.DisableUDP,
        fallback:       lb,
        Hidden:         option.Hidden,
        Icon:           option.Icon,
        policyPriority: make([]priorityRule, 0),
        strategy:       strategy,
    }

	for _, option := range options {
		option(s)
	}

	return s, nil
}

func (s *Smart) GetConfigFilename() string {
    return s.configName
}

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
    proxies := s.GetProxies(true)

    if len(proxies) == 0 {
        return nil, errors.New("no proxy available")
    }

    triedProxies := make(map[string]bool)

    if s.store != nil && s.store.CheckNetworkFailure(s.Name(), s.configName) {
        proxy := s.fallbackToRoundRobin(metadata, proxies)
        if proxy == nil {
            return nil, errors.New("no suitable proxy found in network failure mode")
        }
        triedProxies[proxy.Name()] = true
        for i := 0; i < maxRetries; i++ {
            c, err := proxy.DialContext(ctx, metadata)
            if err == nil {
                return s.wrapConnWithMetric(c, proxy, metadata, 0), nil
            }

            if i == maxRetries-1 {
                break
            }

            proxy = s.selectNextProxy(metadata, proxies, triedProxies)
            if proxy == nil {
                break
            }

            triedProxies[proxy.Name()] = true
        }

        return nil, errors.New("no proxy available")
    }

    proxy := s.selectProxy(metadata, false)
    if proxy == nil {
        proxy = proxies[0]
    }

    triedProxies = make(map[string]bool)
    triedProxies[proxy.Name()] = true

    var finalErr error
    for i := 0; i < maxRetries; i++ {
        start := time.Now()
        c, err := proxy.DialContext(ctx, metadata)
        connectTime := time.Since(start).Milliseconds()

        if err == nil {
            if s.store != nil {
                return s.wrapConnWithMetric(c, proxy, metadata, connectTime), nil
            }
            c.AppendToChains(s)
            return c, nil
        }
        
        finalErr = err
        
        if s.store != nil {
            s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, false, err)
            
            if i == maxRetries-1 {
                domain := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
                s.store.MarkConnectionFailed(s.Name(), s.configName, domain)
                break
            }
        }
        
        triedProxies[proxy.Name()] = true
        
        proxy = s.selectNextProxy(metadata, proxies, triedProxies)
        if proxy == nil {
            break
        }
    }

    return nil, finalErr
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
    proxy := s.selectProxy(metadata, true)
    if proxy == nil {
        return nil, errors.New("no proxy available")
    }

    start := time.Now()
    pc, err = proxy.ListenPacketContext(ctx, metadata)

    if err == nil {
        pc.AppendToChains(s)
    }

    if s.store != nil {
        if err != nil {
            s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, false, err)
        } else {
            connectTime := time.Since(start).Milliseconds()
            s.recordConnectionStats("success", metadata, proxy, connectTime, 0, 0, 0, 0, false, nil)
            pc = s.registerPacketClosureMetricsCallback(pc, proxy, metadata)
        }
    }

    return
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
    proxy := s.selectProxy(metadata, touch)

    if proxy != nil && s.store != nil {
        domain := ""
        if metadata != nil {
            domain = smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
            if domain != "" {
                s.store.StoreUnwrapResult(s.Name(), s.configName, domain, proxy.Name())
            }
        }
    }

    return proxy
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *Smart) wrapConnWithMetric(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
    c.AppendToChains(s)

    c = s.registerClosureMetricsCallback(c, proxy, metadata)
    
    if !N.NeedHandshake(c) {
        s.recordConnectionStats("success", metadata, proxy, connectTime, 0, 0, 0, 0, false, nil)
        return c
    }

    start := time.Now()

    return callback.NewFirstWriteCallBackConn(c, func(err error) {
        latency := time.Since(start).Milliseconds()
        if err == nil {
            s.recordConnectionStats("success", metadata, proxy, connectTime, latency, 0, 0, 0, false, nil)
        } else {
            s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, false, err)
        }
    })
}

func (s *Smart) Set(name string) error {
    var p C.Proxy
    for _, proxy := range s.GetProxies(false) {
        if proxy.Name() == name {
            p = proxy
            break
        }
    }

    if p == nil {
        return errors.New("proxy not exist")
    }

    s.selected = name
    if !p.AliveForTestUrl(s.testUrl) {
        ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
        defer cancel()
        expectedStatus, _ := utils.NewUnsignedRanges[uint16](s.expectedStatus)
        _, _ = p.URLTest(ctx, s.testUrl, expectedStatus)
    }

    return nil
}

func (s *Smart) ForceSet(name string) {
    s.selected = name
}

func (s *Smart) Now() string {
    if s.selected != "" {
        for _, p := range s.GetProxies(false) {
            if p.Name() == s.selected {
                if p.AliveForTestUrl(s.testUrl) {
                    return p.Name()
                }
                break
            }
        }
        s.selected = ""
    }

    return "Smart - Select"
}

func (s *Smart) InitializeCache() {
    cacheFile := cachefile.Cache()
    if cacheFile == nil || cacheFile.DB == nil {
        return
    }

    smartStore := cachefile.NewSmartStore(cacheFile)
    if smartStore == nil {
        return
    }

    s.store = smartStore.GetStore()

    if s.configName == "" {
        s.configName = getConfigFilename()
    }
    
    s.ctx, s.cancel = context.WithCancel(context.Background())

    smartInitOnce.Do(func() {
        s.startTimedTask(5*time.Minute, checkInterval, "Clean up groups", s.cleanupOrphanedGroups, true)
        s.startTimedTask(5*time.Second, cacheParamAdjustInterval, "Cache parameter adjustment", s.store.AdjustCacheParameters, false)
        s.startTimedTask(5*time.Minute, flushQueueInterval, "Queue flush", func() {
            s.store.FlushQueue(false)
        }, false)
    })

    s.startTimedTask(5*time.Minute, checkInterval, "Clean up nodes", s.cleanupOrphanedNodeCache, true)
    s.startTimedTask(10*time.Second, checkInterval, "Preload frequent data", func() {
        proxies := s.GetProxies(false)
        proxyNames := make([]string, 0, len(proxies))
        for _, p := range proxies {
            proxyNames = append(proxyNames, p.Name())
        }
        s.store.PreloadFrequentData(s.Name(), s.configName, proxyNames)
    }, true)
    s.startTimedTask(5*time.Minute, prefetchInterval, "prefetch", s.runPrefetch, false)
    s.startTimedTask(5*time.Minute, rankingInterval, "ranking", s.updateNodeRanking, false)
    s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Recovery check", s.checkAndRecoverDegradedNodes, false)
    s.startTimedTask(10*time.Minute, checkInterval, "long connection processing", func() {
        s.processLongConnections(longConnThreshold)
    }, false)
    s.startTimedTask(5*time.Minute, cleanupInterval, "Expired cleanup", func() {
        _ = s.store.CleanupExpiredStats(s.Name(), s.configName)
    }, false)
    s.startTimedTask(5*time.Minute, cleanupInterval, "OldDomains cleanup", func() {
        _ = s.store.CleanupOldDomains(s.Name(), s.configName)
    }, false)

    if s.useLightGBM {
        s.weightModel = lightgbm.GetModel()
    }
    
    if s.collectData {
        s.dataCollector = lightgbm.GetCollector()
        
        s.startTimedTask(10*time.Minute, 30*time.Minute, "Flush data collector", func() {
            if s.dataCollector != nil {
                s.dataCollector.Flush()
            }
        }, false)
    }
}

func (s *Smart) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()

        jitterRange := 30.0
        intervalJitter := time.Duration(rand.Float64() * jitterRange * float64(time.Second))
        
        adjustedInitialDelay := initialDelay + intervalJitter
        adjustedInterval := interval + intervalJitter
        
        select {
        case <-time.After(adjustedInitialDelay):
        case <-s.ctx.Done():
            return
        }

        task()
        
        if runOnce {
            log.Debugln("[Smart] Task %s for group [%s] set to run once, exiting", 
                taskName, s.Name())
            return
        }
        
        ticker := time.NewTicker(adjustedInterval)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                task()
            case <-s.ctx.Done():
                return
            }
        }
    }()
}

func (s *Smart) runPrefetch() {
	if s.store == nil {
		return
	}

	proxies := s.GetProxies(true)
	proxyMap := make(map[string]string)
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			proxyMap[p.Name()] = p.Name()
		}
	}
	s.store.RunPrefetch(s.Name(), s.configName, proxyMap)
}

func (s *Smart) updateNodeRanking() {
	if s.store == nil {
		return
	}

	log.Debugln("[Smart] Starting node ranking update for policy group [%s]", s.Name())

    proxies := s.GetProxies(false)
    proxyNames := make([]string, 0, len(proxies))
    for _, p := range proxies {
        proxyNames = append(proxyNames, p.Name())
    }
    ranking, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, false, proxyNames)
	if err != nil {
		log.Warnln("[Smart] Failed to update node ranking: %v", err)
		return
	}

	if len(ranking) == 0 {
		log.Debugln("[Smart] Policy group [%s] doesn't have enough data to generate node ranking", s.Name())
		return
	}

	categoryCounts := make(map[string]int)
	for _, rank := range ranking {
		categoryCounts[rank]++
	}

	log.Debugln("[Smart] Policy group [%s] node ranking update completed: %d nodes total (%s: %d, %s: %d, %s: %d)",
		s.Name(), len(ranking),
		smart.RankMostUsed, categoryCounts[smart.RankMostUsed],
		smart.RankOccasional, categoryCounts[smart.RankOccasional],
		smart.RankRarelyUsed, categoryCounts[smart.RankRarelyUsed])
}

// 检查节点屏蔽状态
func (s *Smart) checkAndRecoverDegradedNodes() {
	if s.store == nil {
		return
	}

	stateData, err := s.store.GetNodeStates(s.Name(), s.configName)
	if err != nil {
		return
	}

	nodesToUpdate := make(map[string]*smart.NodeState)

	for nodeName, data := range stateData {
        var state smart.NodeState
        err := json.Unmarshal(data, &state)
        if err != nil {
            continue
        }

        var shouldUpdate bool = false
        
        
        if !state.BlockedUntil.IsZero() && state.BlockedUntil.Before(time.Now()) {
            state.BlockedUntil = time.Time{}
            shouldUpdate = true
            log.Debugln("[Smart] Node [%s] block period expired, unblocking", nodeName)
        }

        if state.Degraded {
            timeSinceLastFailure := time.Since(state.LastFailure)
            
            var recoveryFactor float64
            var shouldRecover bool

            switch {
            case timeSinceLastFailure > failureRecovery30min:
                recoveryFactor = 1.0
                shouldRecover = true
                state.FailureCount = 0
            case timeSinceLastFailure > failureRecovery15min:
                if state.DegradedFactor < 0.9 {
                    recoveryFactor = 0.9
                    shouldRecover = true
                    state.FailureCount = int(float64(state.FailureCount) * 0.5)
                }
            case timeSinceLastFailure > failureRecovery10min:
                if state.DegradedFactor < 0.75 {
                    recoveryFactor = 0.75
                    shouldRecover = true
                    state.FailureCount = int(float64(state.FailureCount) * 0.7)
                }
            case timeSinceLastFailure > failureRecovery5min:
                if state.DegradedFactor < 0.5 {
                    recoveryFactor = 0.5
                    shouldRecover = true
                    state.FailureCount = int(float64(state.FailureCount) * 0.9)
                }
            }

            if shouldRecover {
                shouldUpdate = true
                if recoveryFactor >= 0.99 {
                    state.Degraded = false
                    state.DegradedFactor = 1.0
                } else {
                    state.Degraded = true
                    state.DegradedFactor = recoveryFactor
                }
            }
        } else if state.FailureCount > 0 {
            timeSinceLastFailure := time.Since(state.LastFailure)
            if timeSinceLastFailure > failureRecovery10min {
                state.FailureCount = 0
                shouldUpdate = true
                log.Debugln("[Smart] Reset failure count for node [%s]", nodeName)
            }
        }

        if shouldUpdate {
            nodesToUpdate[nodeName] = &state
        }
    }

	if len(nodesToUpdate) > 0 {
		operations := make([]smart.StoreOperation, 0, len(nodesToUpdate))
        for nodeName, state := range nodesToUpdate {
            data, err := json.Marshal(state)
            if err != nil {
                continue
            }
            operations = append(operations, smart.StoreOperation{
                Type:   smart.OpSaveNodeState,
                Group:  s.Name(),
                Config: s.configName,
                Node:   nodeName,
                Data:   data,
            })
        }
        s.store.BatchSaveConnStats(operations)
	}
}

func (s *Smart) selectProxy(metadata *C.Metadata, touch bool) C.Proxy {
    proxies := s.GetProxies(touch)
    if metadata == nil || len(proxies) == 0 {
        if len(proxies) > 0 {
            return proxies[0]
        }
        return nil
    }

    if s.selected != "" {
        for _, p := range proxies {
            if p.Name() == s.selected {
                if p.AliveForTestUrl(s.testUrl) {
                    return p
                }
                break
            }
        }
    }

    if s.store == nil {
        return proxies[0]
    }

    blockedNodes := make(map[string]bool)
    stateData, _ := s.store.GetNodeStates(s.Name(), s.configName)
    for nodeName, data := range stateData {
        var state smart.NodeState
        if json.Unmarshal(data, &state) == nil {
            if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
                blockedNodes[nodeName] = true
            }
        }
    }
    
    findProxyByName := func(name string) C.Proxy {
        if blockedNodes[name] {
            return nil
        }
        
        for _, p := range proxies {
            if p.Name() == name {
                return p
            }
        }
        return nil
    }

    weightType := smart.WeightTypeTCP
    if metadata.NetWork == C.UDP {
        weightType = smart.WeightTypeUDP
    }
    
    trySelector := func(target string, weightType string) C.Proxy {
        // 检查解析缓存
        if cachedProxyName := s.store.GetUnwrapResult(s.Name(), s.configName, target); cachedProxyName != "" {
            if proxy := findProxyByName(cachedProxyName); proxy != nil {
                s.store.DeleteCacheResult(smart.KeyTypeUnwrap, s.Name(), s.configName, target)
                return proxy
            }
        }
        
        // 检查预解析缓存
        if cachedProxyName := s.store.GetPrefetchResult(s.Name(), s.configName, target, weightType); cachedProxyName != "" {
            if proxy := findProxyByName(cachedProxyName); proxy != nil {
                return proxy
            }
        }
        
        // 实时计算最佳节点
        bestNode, _, _, _ := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, weightType)
        if bestNode != "" {
            if proxy := findProxyByName(bestNode); proxy != nil {
                return proxy
            }
        }
        
        return nil
    }
    
    // 尝试使用域名信息选择
    domain := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
    if domain != "" {
        if proxy := trySelector(domain, weightType); proxy != nil {
            return proxy
        }
    }
    
    // 尝试使用ASN信息选择
    asnNumber := s.getASNCode(metadata)
    if asnNumber != "" {
        asnWeightType := weightType
        if weightType == smart.WeightTypeTCP {
            asnWeightType = smart.WeightTypeTCPASN + ":" + asnNumber
        } else {
            asnWeightType = smart.WeightTypeUDPASN + ":" + asnNumber
        }
        
        if proxy := trySelector(asnNumber, asnWeightType); proxy != nil {
            return proxy
        }
    }

    return s.fallbackToRoundRobin(metadata, proxies)
}

func (s *Smart) selectNextProxy(metadata *C.Metadata, availableProxies []C.Proxy, triedProxies map[string]bool) C.Proxy {
    if s.strategy == "sticky-sessions" {
        for _, p := range availableProxies {
            if !triedProxies[p.Name()] && p.AliveForTestUrl(s.testUrl) {
                return p
            }
        }
        for _, p := range availableProxies {
            if !triedProxies[p.Name()] {
                return p
            }
        }
        return nil
    }

    for i := 0; i < 3; i++ {
        fallbackProxy := s.fallbackToRoundRobin(metadata, availableProxies)
        if fallbackProxy != nil && !triedProxies[fallbackProxy.Name()] {
            return fallbackProxy
        }
    }

    for _, p := range availableProxies {
        if !triedProxies[p.Name()] && p.AliveForTestUrl(s.testUrl) {
            return p
        }
    }

    for _, p := range availableProxies {
        if !triedProxies[p.Name()] {
            return p
        }
    }

    return nil
}

func (s *Smart) fallbackToRoundRobin(metadata *C.Metadata, proxies []C.Proxy) C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	if metadata == nil {
		return proxies[0]
	}

	if s.fallback != nil {
		proxy := s.fallback.Unwrap(metadata, true)
		if proxy != nil {
			return proxy
		}
	}
	return proxies[0]
}

func (s *Smart) SupportUDP() bool {
	if s.disableUDP {
		return false
	}

	return s.selectProxy(nil, false).SupportUDP()
}

func (s *Smart) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, len(proxies))
	for i, proxy := range proxies {
		all[i] = proxy.Name()
	}

	policyPriorityStr := ""
    for _, rule := range s.policyPriority {
        if policyPriorityStr != "" {
            policyPriorityStr += ";"
        }
        policyPriorityStr += fmt.Sprintf("%s:%.2f", rule.pattern, rule.factor)
    }

	return json.Marshal(map[string]any{
        "type":           s.Type().String(),
        "now":            s.Now(),
        "all":            all,
        "testUrl":        s.testUrl,
        "expectedStatus": s.expectedStatus,
        "fixed":          s.selected,
        "hidden":         s.Hidden,
        "icon":           s.Icon,
        "policy-priority": policyPriorityStr,
        "strategy":       s.strategy,
        "useLightGBM":    s.useLightGBM,
        "collectData":    s.collectData,
    })
}

func (s *Smart) cleanupOrphanedGroups() {
	if s.store == nil {
		return
	}

	allProxies := tunnel.Proxies()
	existingSmartGroups := make(map[string]bool)

	for name, proxy := range allProxies {
		if proxy.Type() == C.Smart {
			existingSmartGroups[name] = true
		}
	}

	cachedGroups, err := s.store.GetAllGroupsForConfig(s.configName)
	if err != nil {
		return
	}

	var orphanedGroups []string
	for _, groupName := range cachedGroups {
		if !existingSmartGroups[groupName] {
			orphanedGroups = append(orphanedGroups, groupName)
		}
	}

	if len(orphanedGroups) > 0 {
		for _, group := range orphanedGroups {
			log.Debugln("[Smart] Cleaning up cache data for non-existent policy group [%s]", group)
			err := s.store.FlushByGroup(group, s.configName)
			if err != nil {
				log.Warnln("[Smart] Failed to clean up policy group [%s] cache: %v", group, err)
			}
		}
	}
}

func (s *Smart) cleanupOrphanedNodeCache() {
	if s.store == nil {
		return
	}

	currentProxies := s.GetProxies(true)
	currentNodesMap := make(map[string]bool)
	for _, proxy := range currentProxies {
		currentNodesMap[proxy.Name()] = true
	}

	cachedNodes, err := s.store.GetAllNodesForGroup(s.Name(), s.configName)
	if err != nil {
		return
	}

	var orphanedNodes []string
	for _, nodeName := range cachedNodes {
		if !currentNodesMap[nodeName] {
			orphanedNodes = append(orphanedNodes, nodeName)
		}
	}

	if len(orphanedNodes) > 0 {
		for _, node := range orphanedNodes {
			log.Debugln("[Smart] Cleaning up cache data for non-existent node [%s]", node)
		}

		err := s.store.RemoveNodesData(s.Name(), s.configName, orphanedNodes)
		if err != nil {
			log.Warnln("[Smart] Failed to clean up non-existent node caches: %v", err)
		}
	}
}

func (s *Smart) checkNodeQualityDegradation(domain, proxyName string, newWeight, oldWeight float64, connectionDuration int64, downloadTotal float64, uploadTotal float64, weightType string, asnInfo string) {
    // 识别并处理0流量连接
    if connectionDuration > 0 && downloadTotal == 0 && uploadTotal == 0 && connectionDuration > 1000 {
        degradedWeight := newWeight * 0.3
        
        log.Debugln("[Smart] Zero-traffic connection detected: [%s] for domain [%s], conn time: %dms, forcing weight degradation from %.4f to %.4f (%s)", 
            proxyName, domain, connectionDuration, newWeight, degradedWeight, weightType)
        
        newWeight = degradedWeight
        
        go s.cleanupDegradedNodePreferenceCache(domain, proxyName, newWeight, weightType, asnInfo)
        
        return
    }
    
    // 处理权重显著下降
    if oldWeight > 0 {
        weightChangeRatio := (newWeight - oldWeight) / oldWeight
        
        if weightChangeRatio < -0.3 {
            log.Debugln("[Smart] Node quality degraded: [%s] for domain [%s], weight from %.4f to %.4f (%.1f%%) (%s)", 
                proxyName, domain, oldWeight, newWeight, weightChangeRatio*100, weightType)
            
            go s.cleanupDegradedNodePreferenceCache(domain, proxyName, newWeight, weightType, asnInfo)
        }
    }
}

// ASN权重更新
func (s *Smart) updateAsnWeights(record *smart.AtomicStatsRecord, asnInfo string, weight float64, isUDP bool) {
    parts := strings.SplitN(asnInfo, " ", 2)
    if len(parts) == 0 {
        return
    }
    
    asnNumber := parts[0]
    
    var asnWeightKey string
    if isUDP {
        asnWeightKey = smart.WeightTypeUDPASN + ":" + asnNumber
    } else {
        asnWeightKey = smart.WeightTypeTCPASN + ":" + asnNumber
    }
    
    record.SetWeight(asnWeightKey, weight)
}

// 连接持续时间更新
func (s *Smart) updateConnectionDuration(record *smart.AtomicStatsRecord, connectionDuration int64) {
    durationMinutes := float64(connectionDuration) / 60000.0
    
    currentDuration := record.Get("duration").(float64)
    if currentDuration > 0 {
        record.Set("duration", (currentDuration + durationMinutes) / 2.0)
    } else {
        record.Set("duration", durationMinutes)
    }
}

// 统计数据限制检查
func (s *Smart) checkAndLimitStats(record *smart.AtomicStatsRecord) {
    success := record.Get("success").(int64)
    failure := record.Get("failure").(int64)
    
    if success > 10000 {
        record.Set("success", success/2)
    }
    if failure > 10000 {
        record.Set("failure", failure/2)
    }
    
    connectTime := record.Get("connectTime").(int64)
    if connectTime > 60000 {
        record.Set("connectTime", int64(60000))
    }
    
    latency := record.Get("latency").(int64)
    if latency > 10000 {
        record.Set("latency", int64(10000))
    }
}

// 记录保存
func (s *Smart) saveStatsRecord(cacheKey, domain string, proxy C.Proxy, record *smart.StatsRecord) {
    smart.SetCacheValue(cacheKey, record)
    
    go func() {
        if data, err := json.Marshal(record); err == nil {
            operation := smart.StoreOperation{
                Type:   smart.OpSaveStats,
                Group:  s.Name(),
                Config: s.configName,
                Domain: domain,
                Node:   proxy.Name(),
                Data:   data,
            }
            s.store.BatchSaveConnStats([]smart.StoreOperation{operation})
        }
    }()
}

// 失败连接处理
func (s *Smart) handleFailedConnection(record *smart.StatsRecord, proxyName, cacheKey, domain string) {
    operations := make([]smart.StoreOperation, 0, 2)
    
    if statsData, err := json.Marshal(record); err == nil {
        operations = append(operations, smart.StoreOperation{
            Type:   smart.OpSaveStats,
            Group:  s.Name(),
            Config: s.configName,
            Domain: domain,
            Node:   proxyName,
            Data:   statsData,
        })
    }
    
    nodeStateData, _ := s.store.GetNodeStates(s.Name(), s.configName)
    var nodeState smart.NodeState
    if data, exists := nodeStateData[proxyName]; exists {
        if json.Unmarshal(data, &nodeState) != nil {
            nodeState = smart.NodeState{
                Name:           proxyName,
                FailureCount:   1,
                LastFailure:    time.Now(),
                Degraded:       false,
                DegradedFactor: 1.0,
            }
        } else {
            nodeState.FailureCount++
            nodeState.LastFailure = time.Now()
        }
    } else {
        nodeState = smart.NodeState{
            Name:           proxyName,
            FailureCount:   1,
            LastFailure:    time.Now(),
            Degraded:       false,
            DegradedFactor: 1.0,
        }
    }
    
    if nodeState.FailureCount >= 50 {
        nodeState.Degraded = true
        nodeState.DegradedFactor = 0.1
        nodeState.BlockedUntil = time.Now().Add(5 * time.Minute)
    } else if nodeState.FailureCount >= 30 {
        nodeState.Degraded = true
        nodeState.DegradedFactor = 0.3
    } else if nodeState.FailureCount >= 10 {
        nodeState.Degraded = true
        nodeState.DegradedFactor = 0.5
    }
    
    if nodeStateBytes, err := json.Marshal(&nodeState); err == nil {
        operations = append(operations, smart.StoreOperation{
            Type:   smart.OpSaveNodeState,
            Group:  s.Name(),
            Config: s.configName,
            Node:   proxyName,
            Data:   nodeStateBytes,
        })
    }
    
    if len(operations) > 0 {
        s.store.BatchSaveConnStats(operations)
    }
    
    weightType := smart.WeightTypeTCP
    asnInfo := s.getASNCode(nil)
    var calculatedWeight float64
    if record.Weights != nil {
        calculatedWeight = record.Weights[weightType]
    }
    
    go s.cleanupDegradedNodePreferenceCache(domain, proxyName, calculatedWeight, weightType, asnInfo)
}

// 日志记录
func (s *Smart) logConnectionStats(record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
                                        domain, proxyName string, uploadTotal, downloadTotal, connectionDuration int64, 
                                        asnInfo string, isModelPredicted bool) {
    var tcpAsnWeight, udpAsnWeight float64
    var asnDisplayInfo string
    
    if asnInfo != "" {
        parts := strings.SplitN(asnInfo, " ", 2)
        asnNumber := parts[0]
        
        tcpAsnWeightKey := smart.WeightTypeTCPASN + ":" + asnNumber
        udpAsnWeightKey := smart.WeightTypeUDPASN + ":" + asnNumber
        
        if record.Weights != nil {
            if w, ok := record.Weights[tcpAsnWeightKey]; ok {
                tcpAsnWeight = w
            }
            if w, ok := record.Weights[udpAsnWeightKey]; ok {
                udpAsnWeight = w
            }
        }
        
        asnDisplayInfo = metadata.DstIPASN
    } else {
        asnDisplayInfo = "unknown"
    }
    
    weightSource := "Traditional"
    if isModelPredicted {
        weightSource = "LightGBM"
    }
    
    log.Debugln("[Smart] Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], TCP ASN: [%.4f], UDP ASN: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
        "For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] - ASN: [%s]) "+
        "- Current: (Up: [%.4f MB], Down: [%.4f MB], Duration: [%.2f s]) "+
        "- History: (Success: [%d], Failure: [%d], Connect: [%d ms], Latency: [%d ms], Total Up: [%.4f MB], Total Down: [%.4f MB], Avg Duration: [%.4f min])",
        weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
        s.Name(), proxyName, strings.ToUpper(metadata.NetWork.String()), domain, asnDisplayInfo,
        float64(uploadTotal)/(1024.0*1024.0), float64(downloadTotal)/(1024.0*1024.0), float64(connectionDuration)/1000.0,
        record.Success, record.Failure, record.ConnectTime, record.Latency,
        record.UploadTotal, record.DownloadTotal, record.ConnectionDuration)
}

// 数据收集
func (s *Smart) collectConnectionData(status string, record *smart.StatsRecord, metadata *C.Metadata,
                                           uploadTotal, downloadTotal, connectionDuration int64, baseWeight float64, proxyName string, isModelPredicted bool) {
    var input *lightgbm.ModelInput
    
    if status == "failed" {
        input = lightgbm.CreateModelInputFromStats(
            record.Success, record.Failure, record.ConnectTime, record.Latency,
            metadata.NetWork == C.UDP, metadata.NetWork == C.TCP,
            0, 0, 0, record.LastUsed.Unix(), metadata,
        )
    } else if status == "closed" {
        input = lightgbm.CreateModelInputFromStatsRecord(record, metadata, uploadTotal, downloadTotal, connectionDuration)
    }
    
    if input != nil {
        input.GroupName = s.Name()
        input.NodeName = proxyName

        weightSource := "Traditional"
        if isModelPredicted {
            weightSource = "LightGBM"
        }

        s.dataCollector.AddSample(input, metadata, baseWeight, weightSource)
    }
}

func updateAverageValue(oldValue int64, newValue int64, count int64) int64 {
    var newAverage int64
    
    if oldValue > 0 && count > 1 {
        newAverage = (oldValue*5 + newValue*5) / 6
    } else {
        newAverage = newValue
    }
    
    return newAverage
}

func (s *Smart) recordConnectionStats(status string, metadata *C.Metadata, proxy C.Proxy, 
    connectTime int64, latency int64, uploadTotal int64, downloadTotal int64, 
    connectionDuration int64, fromLongConnProcess bool, err error) {
    
    if s.store == nil || proxy == nil || metadata == nil {
        return
    }

    domain := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
    if domain == "" {
        return
    }
    
    if status == "failed" {
        s.store.MarkConnectionFailed(s.Name(), s.configName, domain)
    } else if status == "success" {
        s.store.MarkConnectionSuccess(s.Name(), s.configName)
    }
    
    cacheKey := smart.FormatCacheKey(smart.KeyTypeStats, s.configName, s.Name(), domain, proxy.Name())
    priorityFactor := s.getPriorityFactor(proxy.Name())
    weightType := smart.WeightTypeTCP
    if metadata.NetWork == C.UDP {
        weightType = smart.WeightTypeUDP
    }
    asnInfo := s.getASNCode(metadata)
    
    lock := smart.GetDomainNodeLock(domain, s.Name(), proxy.Name())
    lock.Lock()
    defer lock.Unlock()
    
    atomicManager := smart.GetAtomicManager()
    if atomicManager == nil {
        return
    }
    
    atomicRecord := atomicManager.GetOrCreateAtomicRecord(cacheKey, s.store, s.Name(), s.configName, domain, proxy.Name())
    if atomicRecord == nil {
        return
    }
    
    var baseWeight, calculatedWeight, oldWeight float64
    var needCheckQuality bool
    var needDataCollection bool
    var isModelPredicted bool
    
    switch status {
    case "success":
        atomicRecord.Add("success", int64(1))
        
        if connectTime > 0 {
            success := atomicRecord.Get("success").(int64)
            oldConnectTime := atomicRecord.Get("connectTime").(int64)
            newConnectTime := updateAverageValue(oldConnectTime, connectTime, success)
            atomicRecord.Set("connectTime", newConnectTime)
        }
        
        if latency > 0 {
            success := atomicRecord.Get("success").(int64)
            oldLatency := atomicRecord.Get("latency").(int64)
            newLatency := updateAverageValue(oldLatency, latency, success)
            atomicRecord.Set("latency", newLatency)
        }
        
        atomicRecord.Set("lastUsed", time.Now().Unix())
        
    case "failed":
        atomicRecord.Add("failure", int64(1))
        atomicRecord.Set("lastUsed", time.Now().Unix())
        
        success := atomicRecord.Get("success").(int64)
        failure := atomicRecord.Get("failure").(int64)
        connectTimeVal := atomicRecord.Get("connectTime").(int64)
        latencyVal := atomicRecord.Get("latency").(int64)
        
        if s.useLightGBM && s.weightModel != nil {
            input := lightgbm.CreateModelInputFromStats(
                success, failure, connectTimeVal, latencyVal,
                metadata.NetWork == C.UDP, metadata.NetWork == C.TCP,
                0, 0, 0, time.Now().Unix(), metadata,
            )
            
            if input != nil {
                calculatedWeight, isModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
            } else {
                calculatedWeight = smart.CalculateWeight(
                    success, failure, connectTimeVal, latencyVal,
                    metadata.NetWork == C.UDP, 0, 0, 0, time.Now().Unix()) * 0.8 * priorityFactor
                isModelPredicted = false
            }
        } else {
            calculatedWeight = smart.CalculateWeight(
                success, failure, connectTimeVal, latencyVal,
                metadata.NetWork == C.UDP, 0, 0, 0, time.Now().Unix()) * 0.8 * priorityFactor
            isModelPredicted = false
        }

        baseWeight = calculatedWeight / priorityFactor
        needDataCollection = s.collectData && s.dataCollector != nil

        atomicRecord.SetWeight(weightType, calculatedWeight)

        if asnInfo != "" {
            s.updateAsnWeights(atomicRecord, asnInfo, calculatedWeight, metadata.NetWork == C.UDP)
        }
        
    case "closed":
        weights := atomicRecord.Get("weights")
        if weights != nil {
            weightsMap := weights.(map[string]float64)
            oldWeight = weightsMap[weightType]
        }
        
        if !fromLongConnProcess {
            uploadTotalMB := float64(uploadTotal) / (1024.0 * 1024.0)
            downloadTotalMB := float64(downloadTotal) / (1024.0 * 1024.0)
            
            atomicRecord.Add("uploadTotal", uploadTotalMB)
            atomicRecord.Add("downloadTotal", downloadTotalMB)
            
            if connectionDuration > 0 {
                s.updateConnectionDuration(atomicRecord, connectionDuration)
            }
        }
        
        success := atomicRecord.Get("success").(int64)
        failure := atomicRecord.Get("failure").(int64)
        connectTimeVal := atomicRecord.Get("connectTime").(int64)
        latencyVal := atomicRecord.Get("latency").(int64)
        
        uploadTotalMB := atomicRecord.Get("uploadTotal").(float64) * 1024 * 1024
        downloadTotalMB := atomicRecord.Get("downloadTotal").(float64) * 1024 * 1024
        durationMs := atomicRecord.Get("duration").(float64) * 60000
        
        if s.useLightGBM && s.weightModel != nil {
            tempRecord := atomicRecord.CreateStatsSnapshot()
            input := lightgbm.CreateModelInputFromStatsRecord(tempRecord, metadata, uploadTotal, downloadTotal, connectionDuration)
            
            if input != nil {
                calculatedWeight, isModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
            } else {
                calculatedWeight = smart.CalculateWeight(
                    success, failure, connectTimeVal, latencyVal,
                    metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, durationMs, time.Now().Unix()) * priorityFactor
                isModelPredicted = false
            }
        } else {
            calculatedWeight = smart.CalculateWeight(
                success, failure, connectTimeVal, latencyVal,
                metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, durationMs, time.Now().Unix()) * priorityFactor
            isModelPredicted = false
        }

        baseWeight = calculatedWeight / priorityFactor
        needDataCollection = s.collectData && s.dataCollector != nil
        
        s.checkAndLimitStats(atomicRecord)

        atomicRecord.SetWeight(weightType, calculatedWeight)

        if asnInfo != "" {
            s.updateAsnWeights(atomicRecord, asnInfo, calculatedWeight, metadata.NetWork == C.UDP)
        }
        
        atomicRecord.Set("lastUsed", time.Now().Unix())
        
        if oldWeight > 0 && calculatedWeight > 0 {
            needCheckQuality = true
        }
    }
    
    statsSnapshot := atomicRecord.CreateStatsSnapshot()
    s.saveStatsRecord(cacheKey, domain, proxy, statsSnapshot)
    
    if status == "failed" {
        s.handleFailedConnection(statsSnapshot, proxy.Name(), cacheKey, domain)
    }
    
    if status == "closed" {
        if needCheckQuality {
            s.checkNodeQualityDegradation(domain, proxy.Name(), calculatedWeight, oldWeight, 
                connectionDuration, float64(downloadTotal), float64(uploadTotal), weightType, asnInfo)
        }
        
        if !fromLongConnProcess {
            s.logConnectionStats(statsSnapshot, metadata, baseWeight, priorityFactor, domain, proxy.Name(), 
                               uploadTotal, downloadTotal, connectionDuration, asnInfo, isModelPredicted)
        }
    }
    
    if needDataCollection {
        s.collectConnectionData(status, statsSnapshot, metadata, uploadTotal, downloadTotal, connectionDuration, baseWeight, proxy.Name(), isModelPredicted)
    }
}

func (s *Smart) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata) C.Conn {
    return callback.NewCloseCallbackConn(c, func() {
        if metadata != nil && metadata.UUID != "" {
            tracker := statistic.DefaultManager.Get(metadata.UUID)
            if tracker != nil {
                info := tracker.Info()
                uploadTotal := info.UploadTotal.Load()
                downloadTotal := info.DownloadTotal.Load()
                connectionDuration := time.Since(info.Start).Milliseconds()
                
                s.recordConnectionStats("closed", metadata, proxy, 0, 0, 
                    uploadTotal, downloadTotal, connectionDuration, false, nil)
                return
            }
        }
    })
}

func (s *Smart) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) C.PacketConn {
    return callback.NewCloseCallbackPacketConn(pc, func() {
        if metadata != nil && metadata.UUID != "" {
            tracker := statistic.DefaultManager.Get(metadata.UUID)
            if tracker != nil {
                info := tracker.Info()
                uploadTotal := info.UploadTotal.Load()
                downloadTotal := info.DownloadTotal.Load()
                connectionDuration := time.Since(info.Start).Milliseconds()
                
                s.recordConnectionStats("closed", metadata, proxy, 0, 0, 
                    uploadTotal, downloadTotal, connectionDuration, false, nil)
                return
            }
        }
    })
}

func (s *Smart) processLongConnections(threshold time.Duration) {
    if s.store == nil {
        return
    }

    _, err, _ := longConnProcessGroup.Do(s.Name()+"-"+s.configName, func() (interface{}, error) {
        statistic.DefaultManager.Range(func(t statistic.Tracker) bool {
            info := t.Info()
            if info == nil || info.Metadata == nil || info.Chain == nil || len(info.Chain) < 2 {
                return true
            }
            
            connectionAge := time.Since(info.Start)
            if connectionAge < threshold {
                return true
            }
            
            var throughThisPolicy bool
            var proxyName string
            
            for i, c := range info.Chain {
                if c == s.Name() {
                    throughThisPolicy = true
                    proxyName = info.Chain[i-1]
                    break
                }
            }
            
            if !throughThisPolicy || proxyName == "" {
                return true
            }
            
            uploadTotal := info.UploadTotal.Load()
            downloadTotal := info.DownloadTotal.Load()
            connectionDuration := connectionAge.Milliseconds()
    
            if uploadTotal > 0 || downloadTotal > 0 {
                for _, p := range s.GetProxies(false) {
                    if p.Name() == proxyName {
                        s.recordConnectionStats("closed", info.Metadata, p, 0, 0, 
                            uploadTotal, downloadTotal, connectionDuration, true, nil)
                        break
                    }
                }
            }
            
            return true
        })
        return nil, nil
    })
    
    if err != nil {
        log.Debugln("[Smart] Error processing long connections: %v", err)
    }
    
}

func (s *Smart) cleanupDegradedNodePreferenceCache(domain string, nodeName string, currentWeight float64, weightType string, asnInfo string) {
    if s.store == nil {
        return
    }

    lock := smart.GetDomainNodeLock(domain, s.Name(), nodeName)
    lock.Lock()
    defer lock.Unlock()

    s.store.DeleteCacheResult(smart.KeyTypePrefetch, s.Name(), s.configName, domain)

    // 处理域名相关缓存
    bestNode, bestWeight, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, domain, weightType)
    if err == nil && bestNode != "" && bestNode != nodeName && bestWeight > currentWeight {
        s.store.StorePrefetchResult(s.Name(), s.configName, domain, weightType, bestNode)
        log.Debugln("[Smart] Added new prefetch result for domain: [%s] -> [%s] (weight: %.4f, type: %s)", 
            domain, bestNode, bestWeight, weightType)
    }

    
    // 处理ASN相关缓存
    if asnInfo != "" {
        parts := strings.SplitN(asnInfo, " ", 2)
        asnNumber := parts[0]
        
        asnWeightType := smart.WeightTypeTCPASN
        if weightType == smart.WeightTypeUDP {
            asnWeightType = smart.WeightTypeUDPASN
        }
        fullAsnWeightType := asnWeightType + ":" + asnNumber

        s.store.DeleteCacheResult(smart.KeyTypePrefetch, s.Name(), s.configName, fullAsnWeightType)
        
        bestNode, bestWeight, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, asnNumber, fullAsnWeightType)
        if err == nil && bestNode != "" && bestNode != nodeName && bestWeight > 0 {
            s.store.StorePrefetchResult(s.Name(), s.configName, asnNumber, fullAsnWeightType, bestNode)
            log.Debugln("[Smart] Added new ASN prefetch result: [%s] -> [%s] (weight: %.4f, type: %s)", 
                asnNumber, bestNode, bestWeight, fullAsnWeightType)
        }
    }
}

func (s *Smart) getPriorityFactor(proxyName string) float64 {
    for _, rule := range s.policyPriority {
        if rule.isRegex && rule.regex != nil {
            if matched, _ := rule.regex.MatchString(proxyName); matched {
                return rule.factor
            }
        } else if strings.Contains(proxyName, rule.pattern) {
            return rule.factor
        }
    }
    return 1.0
}

func smartWithPolicyPriority(policyPriority string) smartOption {
    return func(s *Smart) {
        pairs := strings.Split(policyPriority, ";")
        for _, pair := range pairs {
            kv := strings.SplitN(pair, ":", 2)
            if len(kv) != 2 || strings.TrimSpace(kv[1]) == "" {
                log.Warnln("[Smart] Invalid policy-priority rule: '%s', must be in 'pattern:factor' format and factor is required", pair)
                continue
            }
            pattern := kv[0]
            if factor, err := strconv.ParseFloat(kv[1], 64); err == nil {
                if factor <= 0 {
                    log.Warnln("[Smart] Invalid priority factor %.2f for pattern '%s', factor must be positive", factor, pattern)
                    continue
                }

                rule := priorityRule{
                    pattern: pattern,
                    factor:  factor,
                }

                if re, err := regexp2.Compile(pattern, regexp2.None); err == nil {
                    rule.regex = re
                    rule.isRegex = true
                }

                s.policyPriority = append(s.policyPriority, rule)
            } else {
                log.Warnln("[Smart] Invalid priority factor format for pattern '%s': %v", pattern, err)
            }
        }
    }
}

func smartWithLightGBM(useLightGBM bool) smartOption {
    return func(s *Smart) {
        s.useLightGBM = useLightGBM
    }
}

func smartWithCollectData(collectData bool) smartOption {
    return func(s *Smart) {
        s.collectData = collectData
    }
}

func smartWithStrategy(config map[string]any) string {
    if strategy, ok := config["strategy"].(string); ok {
        return strategy
    }
    return "sticky-sessions"
}

func parseSmartOption(config map[string]any) ([]smartOption, string) {
    opts := []smartOption{}

    strategy := smartWithStrategy(config)

    if elm, ok := config["policy-priority"]; ok {
        if policyPriority, ok := elm.(string); ok {
            opts = append(opts, smartWithPolicyPriority(policyPriority))
        }
    }

    if elm, ok := config["uselightgbm"]; ok {
        if useLightGBM, ok := elm.(bool); ok {
            opts = append(opts, smartWithLightGBM(useLightGBM))
        }
    }

    if elm, ok := config["collectdata"]; ok {
        if collectData, ok := elm.(bool); ok {
            opts = append(opts, smartWithCollectData(collectData))
        }
    }

    return opts, strategy
}

func (s *Smart) getASNCode(metadata *C.Metadata) string {
    if metadata == nil || metadata.DstIPASN == "" {
        return ""
    }
    
    parts := strings.SplitN(metadata.DstIPASN, " ", 2)

    return parts[0]
}

func (s *Smart) Close() error {
    if s.cancel != nil {
        s.cancel()
    }

    s.wg.Wait()
    
    if s.store != nil && !flushQueueOnce.Swap(true) {
        s.store.FlushQueue(true)
    }
    
    lightgbm.CloseAllCollectors()

    return nil
}