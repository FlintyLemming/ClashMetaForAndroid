package smart

import (
    "errors"
    "fmt"
    "math"
    "net"
    "runtime"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/metacubex/mihomo/common/cmd"
    "github.com/metacubex/mihomo/common/lru"
    "github.com/metacubex/mihomo/log"
    "golang.org/x/net/publicsuffix"
)

const (
    OpSaveNodeState StoreOperationType = iota
    OpSaveStats
    OpSavePrefetch
    OpSaveRanking
)

const (
    KeyTypePrefetch = "prefetch" 
    KeyTypeUnwrap   = "unwrap"   
    KeyTypeFailed   = "failed"   
    KeyTypeNode     = "node"     
    KeyTypeStats    = "stats"    
    KeyTypeRanking  = "ranking"  

    WeightTypeTCP    = "tcp"
    WeightTypeUDP    = "udp"
    WeightTypeTCPASN = "tcp_asn"
    WeightTypeUDPASN = "udp_asn"
)

const (
    DefaultMinSampleCount = 2
    RetentionPeriod       = 14 * 24 * time.Hour
    CacheMaxAge           = 21600
    NetworkFailureThreshold = 5
    
    MaxDomainsLimit         = 2000
    MinDomainsLimit         = 300
    MaxCacheSizeLimit       = 2000
    MinCacheSizeLimit       = 300
    MaxBatchThreshLimit     = 1000
    MinBatchThreshLimit     = 100
    MaxPrefetchDomainsLimit = 2000
    MinPrefetchDomainsLimit = 100
    
    MemoryDomainsFactor   = 0.8
    MemoryCacheSizeFactor = 0.7
    MemoryBatchFactor     = 0.7
    MemoryPrefetchFactor  = 0.7
    
    RankMostUsed   = "MostUsed"
    RankOccasional = "OccasionalUsed"
    RankRarelyUsed = "RarelyUsed"
)

type StoreOperationType int

var bucketSmartStats = []byte("smart_stats")

var (
    globalInitInstances = make(map[string]bool)
    globalInitLock      sync.Mutex

    globalOperationQueue []StoreOperation
    globalQueueMutex     sync.RWMutex

    globalCacheParams struct {
        BatchSaveThreshold int
        MaxDomains         int
        PrefetchLimit      int
        CacheMaxSize       int
        MemoryLimit        float64
        LastMemoryUsage    float64
        mutex              sync.RWMutex
    }
    
    dataCache *lru.LruCache[string, interface{}]
    globalCacheLock sync.RWMutex
    
    cachedMemoryLimit float64
    memoryLimitOnce   sync.Once

    opMapPool = sync.Pool{
        New: func() interface{} {
            return make(map[string][]byte, 64)
        },
    }
    
    cacheUpdatePool = sync.Pool{
        New: func() interface{} {
            return make(map[string]interface{}, 64)
        },
    }

    domainValidityCache *lru.LruCache[string, bool]

    StatsCache *lru.LruCache[string, *StatsRecord]

    presetSceneParams = map[string]SceneParams{
        "interactive": {0.4, 0.2, 0.4, 1.2, 1.0, 1.3, 0.005},
        "streaming":   {0.5, 0.1, 0.4, 1.5, 0.8, 1.2, 0.008},
        "transfer":    {0.6, 0.2, 0.2, 1.8, 0.7, 0.9, 0.01},
        "web":         {0.5, 0.3, 0.2, 0.8, 0.6, 1.0, 0.012},
    }
)

type (
    StoreOperation struct {
        Type   StoreOperationType
        Group  string
        Config string
        Domain string
        Node   string
        Data   []byte
    }
    
    DomainRecord struct {
        Key      string    `json:"key"`       
        NodeName string    `json:"node_name"` 
        Domain   string    `json:"domain"`    
        LastUsed time.Time `json:"last_used"` 
    }
    
    StatsRecord struct {
        Success           int64               `json:"success"`
        Failure           int64               `json:"failure"`
        ConnectTime       int64               `json:"connect_time"`
        Latency           int64               `json:"latency"`
        LastUsed          time.Time           `json:"last_used"`
        Weights           map[string]float64  `json:"weights"`
        UploadTotal       float64             `json:"upload_total"`
        DownloadTotal     float64             `json:"download_total"`
        ConnectionDuration float64            `json:"connection_duration"`
    }
    
    NodeState struct {
        Name           string    `json:"name"`
        FailureCount   int       `json:"failure_count"`
        LastFailure    time.Time `json:"last_failure"`
        BlockedUntil   time.Time `json:"blocked_until"`
        Degraded       bool      `json:"degraded"`
        DegradedFactor float64   `json:"degraded_factor"` 
    }

    RankingData struct {
        Ranking     map[string]string `json:"ranking"`
        LastUpdated time.Time         `json:"last_updated"`
    }

    SceneParams struct {
        successRateWeight  float64
        connectTimeWeight  float64
        latencyWeight      float64
        trafficWeight      float64
        durationWeight     float64
        qualityWeight      float64
        timeDecayRate      float64
    }
)

func InitializeGlobalParams() {
    InitializeCache()
}

// 格式化缓存键
func FormatCacheKey(keyType, config, group string, parts ...string) string {
    elements := []string{keyType, config, group}
    elements = append(elements, parts...)
    return strings.Join(elements, ":")
}

// 格式化数据库键
func FormatDBKey(first string, parts ...string) string {
    elements := make([]string, 0, len(parts)+1)
    elements = append(elements, first)
    
    for _, part := range parts {
        if part != "" {
            elements = append(elements, part)
        }
    }
    
    return strings.Join(elements, "/")
}

// 获取有效域名
func GetEffectiveDomain(host string, dstIP string) string {
    if domainValidityCache == nil {
        return host
    }
    
    if host != "" {
        cacheKey := "domain:" + host
        if valid, ok := domainValidityCache.Get(cacheKey); ok {
            if valid {
                return host
            }
            goto tryIP
        }
        
        if ip := net.ParseIP(host); ip != nil {
            domainValidityCache.Set(cacheKey, true)
            return ip.String()
        }

        if eTLD, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
            domainValidityCache.Set(cacheKey, true)
            return eTLD
        }
        
        domainValidityCache.Set(cacheKey, false)
        return host
    }

tryIP:
    if dstIP != "" {
        return dstIP
    }

    return ""
}

// 限制值在指定范围内
func ClampValue(value, min, max int) int {
    if value < min {
        return min
    }
    if value > max {
        return max
    }
    return value
}

// 根据系统内存计算限制
func CalculateMemoryBasedLimit(memUsage float64, min, max int, factor float64) int {
    if memUsage < 0 {
        memUsage = 0
    } else if memUsage > 100 {
        memUsage = 100
    }
    
    availFactor := 1.0 - (memUsage / 100.0)
    
    value := min + int(float64(max-min) * availFactor * factor)
    
    return ClampValue(value, min, max)
}

// 获取批量保存阈值
func GetBatchSaveThreshold() int {
    globalCacheParams.mutex.RLock()
    defer globalCacheParams.mutex.RUnlock()
    
    if globalCacheParams.BatchSaveThreshold <= 0 {
        return MinBatchThreshLimit
    }
    
    return globalCacheParams.BatchSaveThreshold
}

// 获取系统内存使用情况
func GetSystemMemoryUsage() float64 {
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)
    inuse := float64(memStats.Alloc) / (1024 * 1024)
    
    globalCacheParams.mutex.RLock()
    memLimit := globalCacheParams.MemoryLimit
    globalCacheParams.mutex.RUnlock()
    
    if memLimit <= 0 {
        memLimit = 100
    }
    
    usagePercent := math.Min(inuse / memLimit * 100.0, 100.0)
    return usagePercent
}

// 检查当前实例是否是特定配置的第一个实例
func IsFirstInstanceForConfig(config string) bool {
    globalInitLock.Lock()
    defer globalInitLock.Unlock()

    key := fmt.Sprintf("%s", config)
    if globalInitInstances[key] {
        return false
    }

    globalInitInstances[key] = true
    return true
}

func getSystemMemoryLimit() float64 {
    memoryLimitOnce.Do(func() {
        var memTotal float64 = 100.0
        var output string
        var err error
        
        if runtime.GOOS == "windows" {
            output, err = cmd.ExecCmd("wmic OS get TotalVisibleMemorySize")
            if err == nil {
                lines := strings.Split(output, "\n")
                if len(lines) >= 2 {
                    memStr := strings.TrimSpace(lines[1])
                    memKB, parseErr := strconv.ParseFloat(memStr, 64)
                    if parseErr == nil {
                        memTotal = memKB / 1024.0
                    }
                }
            }
            } else if runtime.GOOS == "linux" || runtime.GOOS == "android" || runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" {
                output, err = cmd.ExecCmd("grep MemTotal /proc/meminfo")
                if err == nil {
                    parts := strings.Fields(output)
                    if len(parts) >= 2 {
                        memStr := strings.TrimSuffix(parts[1], "kB")
                        memStr = strings.TrimSpace(memStr)
                        memKB, parseErr := strconv.ParseFloat(memStr, 64)
                        if parseErr == nil {
                            memTotal = memKB / 1024.0
                        }
                    }
                }
            }
        
        memTotal = memTotal / 4.0
        
        if memTotal < 100.0 {
            cachedMemoryLimit = 100.0
        } else if memTotal > 512.0 {
            cachedMemoryLimit = 512.0
        } else {
            cachedMemoryLimit = memTotal
        }
    })
    
    return cachedMemoryLimit
}

// 按级别刷新缓存
func (s *Store) FlushByLevel(level string, config string, group string) error {
    if level == "" {
        return errors.New("flush level cannot be empty")
    }

    globalQueueMutex.Lock()
    if level == "all" {
        globalOperationQueue = make([]StoreOperation, 0, MinBatchThreshLimit)
    } else if level == "config" || level == "group" {
        newQueue := make([]StoreOperation, 0, MinBatchThreshLimit)
        for _, op := range globalOperationQueue {
            if level == "config" && op.Config != config {
                newQueue = append(newQueue, op)
            } else if level == "group" && !(op.Group == group && op.Config == config) {
                newQueue = append(newQueue, op)
            }
        }
        globalOperationQueue = newQueue
    }
    globalQueueMutex.Unlock()

    ClearCacheByLevel(level, config, group)

    s.failureStatusLock.Lock()
    if level == "all" {
        s.networkFailureStatus = make(map[string]bool)
        s.successCount = make(map[string]int)
        s.lastNetworkFailure = make(map[string]time.Time)
    } else if level == "group" {
        groupKey := fmt.Sprintf("%s:%s", group, config)
        delete(s.networkFailureStatus, groupKey)
        delete(s.successCount, groupKey)
        delete(s.lastNetworkFailure, groupKey)
    } else if level == "config" {
        for key := range s.networkFailureStatus {
            if strings.Contains(key, ":"+config) {
                delete(s.networkFailureStatus, key)
                delete(s.successCount, key)
                delete(s.lastNetworkFailure, key)
            }
        }
    }
    s.failureStatusLock.Unlock()
    
    if level == "all" {
        return s.DeleteByPath("smart")
    } else if level == "config" {
        s.DeleteByPath(FormatDBKey("smart", KeyTypeStats, config))
        s.DeleteByPath(FormatDBKey("smart", KeyTypeNode, config))
        s.DeleteByPath(FormatDBKey("smart", KeyTypeRanking, config))
        s.DeleteByPath(FormatDBKey("smart", KeyTypePrefetch, config))
    } else if level == "group" {
        s.DeleteByPath(FormatDBKey("smart", KeyTypeStats, config, group, ""))
        s.DeleteByPath(FormatDBKey("smart", KeyTypeNode, config, group, ""))
        s.DeleteByPath(FormatDBKey("smart", KeyTypeRanking, config, group, ""))
        s.DeleteByPath(FormatDBKey("smart", KeyTypePrefetch, config, group, ""))
    }
    
    return nil
}

// 清空所有缓存
func (s *Store) FlushAll() error {
    log.Debugln("[SmartStore] Starting FlushAll, current queue length: %d", len(globalOperationQueue))
    err := s.FlushByLevel("all", "", "")
    if err == nil {
        log.Debugln("[SmartStore] All Smart data cleared")
    }
    return err
}

// 按配置清空缓存
func (s *Store) FlushByConfig(config string) error {
    err := s.FlushByLevel("config", config, "")
    if err == nil {
        log.Debugln("[SmartStore] All data for config [%s] cleared", config)
    }
    return err
}

func (s *Store) FlushByGroup(group, config string) error {
    err := s.FlushByLevel("group", config, group)
    if err == nil {
        log.Debugln("[SmartStore] All data for group [%s] config [%s] cleared", group, config)
    }
    return err
}