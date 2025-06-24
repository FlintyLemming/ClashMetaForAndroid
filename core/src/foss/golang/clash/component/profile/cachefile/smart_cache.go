package cachefile

import (
    "sync"

    "github.com/metacubex/bbolt"
    "github.com/metacubex/mihomo/component/smart"
    "github.com/metacubex/mihomo/log"
)

var (
    smartInitOnce sync.Once
    smartStore *smart.Store
)

type SmartStore struct {
    store *smart.Store
}

func NewSmartStore(cache *CacheFile) *SmartStore {
    if cache == nil || cache.DB == nil {
        return nil
    }

    err := cache.DB.Update(func(tx *bbolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists(bucketSmartStats)
        return err
    })

    if err != nil {
        log.Warnln("[SmartStore] Failed to create bucket: %v", err)
        return nil
    }


    smartInitOnce.Do(func() {
        smart.InitializeGlobalParams()
        smartStore = smart.NewStore(cache.DB)
    })
    
    return &SmartStore{
        store: smartStore,
    }
}

func (s *SmartStore) GetStore() *smart.Store {
    return s.store
}