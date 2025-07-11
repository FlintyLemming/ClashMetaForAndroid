package executor

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
	_ "unsafe"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/component/auth"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/geodata"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/component/iface"
	"github.com/metacubex/mihomo/component/keepalive"
	"github.com/metacubex/mihomo/component/profile"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/resource"
	"github.com/metacubex/mihomo/component/sniffer"
	tlsC "github.com/metacubex/mihomo/component/tls"
	"github.com/metacubex/mihomo/component/trie"
	"github.com/metacubex/mihomo/component/updater"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/dns"
	"github.com/metacubex/mihomo/listener"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/inner"
	"github.com/metacubex/mihomo/listener/tproxy"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/ntp"
	"github.com/metacubex/mihomo/tunnel"
)

var mux sync.Mutex

func readConfig(path string) ([]byte, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("configuration file %s is empty", path)
	}

	return data, err
}

// Parse config with default config path
func Parse() (*config.Config, error) {
	return ParseWithPath(C.Path.Config())
}

// ParseWithPath parse config with custom config path
func ParseWithPath(path string) (*config.Config, error) {
	buf, err := readConfig(path)
	if err != nil {
		return nil, err
	}

	return ParseWithBytes(buf)
}

// ParseWithBytes config with buffer
func ParseWithBytes(buf []byte) (*config.Config, error) {
	return config.Parse(buf)
}

// ApplyConfig dispatch configure to all parts without ExternalController
func ApplyConfig(cfg *config.Config, force bool) {
	mux.Lock()
	defer mux.Unlock()
	log.SetLevel(cfg.General.LogLevel)

	tunnel.OnSuspend()

	ca.ResetCertificate()
	for _, c := range cfg.TLS.CustomTrustCert {
		if err := ca.AddCertificate(c); err != nil {
			log.Warnln("%s\nadd error: %s", c, err.Error())
		}
	}

	updateExperimental(cfg.Experimental)
	updateUsers(cfg.Users)
	updateProxies(cfg.Proxies, cfg.Providers)
	updateRules(cfg.Rules, cfg.SubRules, cfg.RuleProviders)
	updateSniffer(cfg.Sniffer)
	updateHosts(cfg.Hosts)
	updateGeneral(cfg.General, true)
	updateNTP(cfg.NTP)
	updateDNS(cfg.DNS, cfg.General.IPv6)
	updateListeners(cfg.General, cfg.Listeners, force)
	updateTun(cfg.General) // tun should not care "force"
	updateIPTables(cfg)
	updateTunnels(cfg.Tunnels)

	tunnel.OnInnerLoading()

	initInnerTcp()
	loadProvider(cfg.Providers)
	updateProfile(cfg)
	loadProvider(cfg.RuleProviders)
	runtime.GC()
	tunnel.OnRunning()
	updateUpdater(cfg)

	initializeSmartGroups(cfg.Proxies)
	
	resolver.ResetConnection()
}

func initInnerTcp() {
	inner.New(tunnel.Tunnel)
}

func GetGeneral() *config.General {
	ports := listener.GetPorts()
	var authenticator []string
	if auth := authStore.Default.Authenticator(); auth != nil {
		authenticator = auth.Users()
	}

	general := &config.General{
		Inbound: config.Inbound{
			Port:              ports.Port,
			SocksPort:         ports.SocksPort,
			RedirPort:         ports.RedirPort,
			TProxyPort:        ports.TProxyPort,
			MixedPort:         ports.MixedPort,
			Tun:               listener.GetTunConf(),
			TuicServer:        listener.GetTuicConf(),
			ShadowSocksConfig: ports.ShadowSocksConfig,
			VmessConfig:       ports.VmessConfig,
			Authentication:    authenticator,
			SkipAuthPrefixes:  inbound.SkipAuthPrefixes(),
			LanAllowedIPs:     inbound.AllowedIPs(),
			LanDisAllowedIPs:  inbound.DisAllowedIPs(),
			AllowLan:          listener.AllowLan(),
			BindAddress:       listener.BindAddress(),
			InboundTfo:        inbound.Tfo(),
			InboundMPTCP:      inbound.MPTCP(),
		},
		Mode:         tunnel.Mode(),
		UnifiedDelay: adapter.UnifiedDelay.Load(),
		LogLevel:     log.Level(),
		IPv6:         !resolver.DisableIPv6,
		Interface:    dialer.DefaultInterface.Load(),
		RoutingMark:  int(dialer.DefaultRoutingMark.Load()),
		GeoXUrl: config.GeoXUrl{
			GeoIp:   geodata.GeoIpUrl(),
			Mmdb:    geodata.MmdbUrl(),
			ASN:     geodata.ASNUrl(),
			GeoSite: geodata.GeoSiteUrl(),
		},
		GeoAutoUpdate:           updater.GeoAutoUpdate(),
		GeoUpdateInterval:       updater.GeoUpdateInterval(),
		GeodataMode:             geodata.GeodataMode(),
		GeodataLoader:           geodata.LoaderName(),
		GeositeMatcher:          geodata.SiteMatcherName(),
		TCPConcurrent:           dialer.GetTcpConcurrent(),
		FindProcessMode:         tunnel.FindProcessMode(),
		Sniffing:                tunnel.IsSniffing(),
		GlobalClientFingerprint: tlsC.GetGlobalFingerprint(),
		GlobalUA:                mihomoHttp.UA(),
		ETagSupport:             resource.ETag(),
		KeepAliveInterval:       int(keepalive.KeepAliveInterval() / time.Second),
		KeepAliveIdle:           int(keepalive.KeepAliveIdle() / time.Second),
		DisableKeepAlive:        keepalive.DisableKeepAlive(),
	}

	return general
}

func updateListeners(general *config.General, listeners map[string]C.InboundListener, force bool) {
	listener.PatchInboundListeners(listeners, tunnel.Tunnel, true)
	if !force {
		return
	}

	allowLan := general.AllowLan
	listener.SetAllowLan(allowLan)
	inbound.SetSkipAuthPrefixes(general.SkipAuthPrefixes)
	inbound.SetAllowedIPs(general.LanAllowedIPs)
	inbound.SetDisAllowedIPs(general.LanDisAllowedIPs)

	bindAddress := general.BindAddress
	listener.SetBindAddress(bindAddress)
	listener.ReCreateHTTP(general.Port, tunnel.Tunnel)
	listener.ReCreateSocks(general.SocksPort, tunnel.Tunnel)
	listener.ReCreateRedir(general.RedirPort, tunnel.Tunnel)
	listener.ReCreateTProxy(general.TProxyPort, tunnel.Tunnel)
	listener.ReCreateMixed(general.MixedPort, tunnel.Tunnel)
	listener.ReCreateShadowSocks(general.ShadowSocksConfig, tunnel.Tunnel)
	listener.ReCreateVmess(general.VmessConfig, tunnel.Tunnel)
	listener.ReCreateTuic(general.TuicServer, tunnel.Tunnel)
}

func updateTun(general *config.General) {
	listener.ReCreateTun(general.Tun, tunnel.Tunnel)
}

func updateExperimental(c *config.Experimental) {
	if c.QUICGoDisableGSO {
		_ = os.Setenv("QUIC_GO_DISABLE_GSO", strconv.FormatBool(true))
	}
	if c.QUICGoDisableECN {
		_ = os.Setenv("QUIC_GO_DISABLE_ECN", strconv.FormatBool(true))
	}
	resolver.SetIP4PEnable(c.IP4PEnable)
}

func updateNTP(c *config.NTP) {
	if c.Enable {
		ntp.ReCreateNTPService(
			net.JoinHostPort(c.Server, strconv.Itoa(c.Port)),
			time.Duration(c.Interval),
			c.DialerProxy,
			c.WriteToSystem,
		)
	}
}

func updateDNS(c *config.DNS, generalIPv6 bool) {
	if !c.Enable {
		resolver.DefaultResolver = nil
		resolver.DefaultHostMapper = nil
		resolver.DefaultLocalServer = nil
		resolver.ProxyServerHostResolver = nil
		resolver.DirectHostResolver = nil
		dns.ReCreateServer("", nil, nil)
		return
	}
	cfg := dns.Config{
		Main:                 c.NameServer,
		Fallback:             c.Fallback,
		IPv6:                 c.IPv6 && generalIPv6,
		IPv6Timeout:          c.IPv6Timeout,
		EnhancedMode:         c.EnhancedMode,
		Pool:                 c.FakeIPRange,
		Hosts:                c.Hosts,
		FallbackIPFilter:     c.FallbackIPFilter,
		FallbackDomainFilter: c.FallbackDomainFilter,
		Default:              c.DefaultNameserver,
		Policy:               c.NameServerPolicy,
		ProxyServer:          c.ProxyServerNameserver,
		DirectServer:         c.DirectNameServer,
		DirectFollowPolicy:   c.DirectFollowPolicy,
		CacheAlgorithm:       c.CacheAlgorithm,
	}

	r := dns.NewResolver(cfg)
	m := dns.NewEnhancer(cfg)

	// reuse cache of old host mapper
	if old := resolver.DefaultHostMapper; old != nil {
		m.PatchFrom(old.(*dns.ResolverEnhancer))
	}

	resolver.DefaultResolver = r
	resolver.DefaultHostMapper = m
	resolver.DefaultLocalServer = dns.NewLocalServer(r.Resolver, m)
	resolver.UseSystemHosts = c.UseSystemHosts

	if r.ProxyResolver.Invalid() {
		resolver.ProxyServerHostResolver = r.ProxyResolver
	} else {
		resolver.ProxyServerHostResolver = r.Resolver
	}

	if r.DirectResolver.Invalid() {
		resolver.DirectHostResolver = r.DirectResolver
	} else {
		resolver.DirectHostResolver = r.Resolver
	}

	dns.ReCreateServer(c.Listen, r.Resolver, m)
}

func updateHosts(tree *trie.DomainTrie[resolver.HostValue]) {
	resolver.DefaultHosts = resolver.NewHosts(tree)
}

func updateProxies(proxies map[string]C.Proxy, providers map[string]provider.ProxyProvider) {
	tunnel.UpdateProxies(proxies, providers)
}

func updateRules(rules []C.Rule, subRules map[string][]C.Rule, ruleProviders map[string]provider.RuleProvider) {
	tunnel.UpdateRules(rules, subRules, ruleProviders)
}

func loadProvider[P provider.Provider](providers map[string]P) {
	load := func(pv P) {
		name := pv.Name()
		if pv.VehicleType() == provider.Compatible {
			log.Infoln("Start initial compatible provider %s", name)
		} else {
			log.Infoln("Start initial provider %s", name)
		}

		if err := pv.Initial(); err != nil {
			switch pv.Type() {
			case provider.Proxy:
				{
					log.Errorln("initial proxy provider %s error: %v", name, err)
				}
			case provider.Rule:
				{
					log.Errorln("initial rule provider %s error: %v", name, err)
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	ch := make(chan struct{}, concurrentCount)
	for _, pv := range providers {
		pv := pv
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			defer func() { <-ch; wg.Done() }()
			load(pv)
		}()
	}
	wg.Wait()
}

func updateSniffer(snifferConfig *sniffer.Config) {
	dispatcher, err := sniffer.NewDispatcher(snifferConfig)
	if err != nil {
		log.Warnln("initial sniffer failed, err:%v", err)
	}

	tunnel.UpdateSniffer(dispatcher)

	if snifferConfig.Enable {
		log.Infoln("Sniffer is loaded and working")
	} else {
		log.Infoln("Sniffer is closed")
	}
}

func updateTunnels(tunnels []LC.Tunnel) {
	listener.PatchTunnel(tunnels, tunnel.Tunnel)
}

func updateUpdater(cfg *config.Config) {
	general := cfg.General
	updater.SetGeoAutoUpdate(general.GeoAutoUpdate)
	updater.SetGeoUpdateInterval(general.GeoUpdateInterval)

	controller := cfg.Controller
	updater.DefaultUiUpdater = updater.NewUiUpdater(controller.ExternalUI, controller.ExternalUIURL, controller.ExternalUIName)
	updater.DefaultUiUpdater.AutoDownloadUI()
}

//go:linkname temporaryUpdateGeneral github.com/metacubex/mihomo/config.temporaryUpdateGeneral
func temporaryUpdateGeneral(general *config.General) func() {
	oldGeneral := GetGeneral()
	updateGeneral(general, false)
	return func() {
		updateGeneral(oldGeneral, false)
	}
}

func updateGeneral(general *config.General, logging bool) {
	tunnel.SetMode(general.Mode)
	tunnel.SetFindProcessMode(general.FindProcessMode)
	resolver.DisableIPv6 = !general.IPv6

	dialer.SetTcpConcurrent(general.TCPConcurrent)
	if logging && general.TCPConcurrent {
		log.Infoln("Use tcp concurrent")
	}

	inbound.SetTfo(general.InboundTfo)
	inbound.SetMPTCP(general.InboundMPTCP)

	keepalive.SetKeepAliveIdle(time.Duration(general.KeepAliveIdle) * time.Second)
	keepalive.SetKeepAliveInterval(time.Duration(general.KeepAliveInterval) * time.Second)
	keepalive.SetDisableKeepAlive(general.DisableKeepAlive)

	adapter.UnifiedDelay.Store(general.UnifiedDelay)

	dialer.DefaultInterface.Store(general.Interface)
	dialer.DefaultRoutingMark.Store(int32(general.RoutingMark))
	if logging && general.RoutingMark > 0 {
		log.Infoln("Use routing mark: %#x", general.RoutingMark)
	}

	iface.FlushCache()

	geodata.SetGeodataMode(general.GeodataMode)
	geodata.SetLoader(general.GeodataLoader)
	geodata.SetSiteMatcher(general.GeositeMatcher)
	geodata.SetGeoIpUrl(general.GeoXUrl.GeoIp)
	geodata.SetGeoSiteUrl(general.GeoXUrl.GeoSite)
	geodata.SetMmdbUrl(general.GeoXUrl.Mmdb)
	geodata.SetASNUrl(general.GeoXUrl.ASN)
	mihomoHttp.SetUA(general.GlobalUA)
	resource.SetETag(general.ETagSupport)

	tlsC.SetGlobalFingerprint(general.GlobalClientFingerprint)
}

func updateUsers(users []auth.AuthUser) {
	authenticator := auth.NewAuthenticator(users)
	authStore.Default.SetAuthenticator(authenticator)
	if authenticator != nil {
		log.Infoln("Authentication of local server updated")
	}
}

func updateProfile(cfg *config.Config) {
	profileCfg := cfg.Profile

	profile.StoreSelected.Store(profileCfg.StoreSelected)
	if profileCfg.StoreSelected {
		patchSelectGroup(cfg.Proxies)
	}
}

func patchSelectGroup(proxies map[string]C.Proxy) {
	mapping := cachefile.Cache().SelectedMap()
	if mapping == nil {
		return
	}

	for name, proxy := range proxies {
		outbound, ok := proxy.(C.Proxy)
		if !ok {
			continue
		}

		selector, ok := outbound.Adapter().(outboundgroup.SelectAble)
		if !ok {
			continue
		}

		selected, exist := mapping[name]
		if !exist {
			continue
		}

		selector.ForceSet(selected)
	}
}

func updateIPTables(cfg *config.Config) {
	tproxy.CleanupTProxyIPTables()

	iptables := cfg.IPTables
	if runtime.GOOS != "linux" || !iptables.Enable {
		return
	}

	var err error
	defer func() {
		if err != nil {
			log.Errorln("[IPTABLES] setting iptables failed: %s", err.Error())
			os.Exit(2)
		}
	}()

	if cfg.General.Tun.Enable {
		err = fmt.Errorf("when tun is enabled, iptables cannot be set automatically")
		return
	}

	var (
		inboundInterface = "lo"
		bypass           = iptables.Bypass
		tProxyPort       = cfg.General.TProxyPort
		dnsCfg           = cfg.DNS
		DnsRedirect      = iptables.DnsRedirect

		dnsPort netip.AddrPort
	)

	if tProxyPort == 0 {
		err = fmt.Errorf("tproxy-port must be greater than zero")
		return
	}

	if DnsRedirect {
		if !dnsCfg.Enable {
			err = fmt.Errorf("DNS server must be enable")
			return
		}

		dnsPort, err = netip.ParseAddrPort(dnsCfg.Listen)
		if err != nil {
			err = fmt.Errorf("DNS server must be correct")
			return
		}
	}

	if iptables.InboundInterface != "" {
		inboundInterface = iptables.InboundInterface
	}

	dialer.DefaultRoutingMark.CompareAndSwap(0, 2158)

	err = tproxy.SetTProxyIPTables(inboundInterface, bypass, uint16(tProxyPort), DnsRedirect, dnsPort.Port())
	if err != nil {
		return
	}

	log.Infoln("[IPTABLES] Setting iptables completed")
}

func initializeSmartGroups(proxies map[string]C.Proxy) {
	closeSmartGroups()
    for _, proxy := range proxies {
        if proxy.Type() == C.Smart {
            if smart, ok := proxy.Adapter().(*outboundgroup.Smart); ok {
                log.Infoln("[Smart] Initializing Smart Group: %s", proxy.Name())
                smart.InitializeCache()
            }
        }
    }
}

func closeSmartGroups() {
    for _, proxy := range tunnel.Proxies() {
        if proxy.Type() == C.Smart {
            adapter := proxy.Adapter()
            if smart, ok := adapter.(*outboundgroup.Smart); ok {
                smart.Close()
            }
        }
    }
}

func Shutdown() {
	listener.Cleanup()
	tproxy.CleanupTProxyIPTables()
	resolver.StoreFakePoolState()

	closeSmartGroups()

	log.Warnln("Mihomo shutting down")
}