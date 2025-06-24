package route

import (
	"context"
	"net/http"
	"strconv"
	"time"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/log"
)

func groupRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getGroups)

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName)
		r.Get("/", getGroup)
		r.Get("/delay", getGroupDelay)
		r.Get("/weights", getGroupWeights)
	})
	return r
}

func getGroups(w http.ResponseWriter, r *http.Request) {
	var gs []C.Proxy
	for _, p := range tunnel.Proxies() {
		if _, ok := p.Adapter().(C.Group); ok {
			gs = append(gs, p)
		}
	}
	render.JSON(w, r, render.M{
		"proxies": gs,
	})
}

func getGroup(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	if _, ok := proxy.Adapter().(C.Group); ok {
		render.JSON(w, r, proxy)
		return
	}
	render.Status(r, http.StatusNotFound)
	render.JSON(w, r, ErrNotFound)
}

func getGroupDelay(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	group, ok := proxy.Adapter().(C.Group)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	if selectAble, ok := proxy.Adapter().(outboundgroup.SelectAble); ok && proxy.Type() != C.Selector {
		selectAble.ForceSet("")
		cachefile.Cache().SetSelected(proxy.Name(), "")
	}

	query := r.URL.Query()
	url := query.Get("url")
	timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 32)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](query.Get("expected"))
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Millisecond*time.Duration(timeout))
	defer cancel()

	dm, err := group.URLTest(ctx, url, expectedStatus)
	if err != nil {
		render.Status(r, http.StatusGatewayTimeout)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.JSON(w, r, dm)
}

func getGroupWeights(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	smartGroup, ok := proxy.Adapter().(*outboundgroup.Smart)
	if !ok {
		log.Debugln("[Smart] Failed to request weight ranking: Not a Smart group (actual type: %T)", proxy.Adapter())
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"error": fmt.Sprintf("Not a Smart group (actual type: %T)", proxy.Adapter()),
		})
		return
	}
	
	configName := smartGroup.GetConfigFilename()
	groupName := smartGroup.Name()
	
	db := cachefile.Cache()
	if db == nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"error": "Cache not available",
		})
		return
	}
	
	smartStore := cachefile.NewSmartStore(db)
	if smartStore == nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"error": "Smart cache not available",
		})
		return
	}

	proxies := smartGroup.GetProxies(false)
	proxyNames := make([]string, 0, len(proxies))
	for _, p := range proxies {
		proxyNames = append(proxyNames, p.Name())
	}

	if len(proxyNames) == 0 {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"message": "No available proxies in this group",
		})
		return
	}
	
	weights, err := smartStore.GetStore().GetNodeWeightRanking(groupName, configName, true, proxyNames)

	if err != nil {
		log.Warnln("[Smart] Failed to get weight ranking: %s", err.Error())
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"error": "Failed to get weight ranking: " + err.Error(),
		})
		return
	}
	
	if len(weights) == 0 {
		log.Debugln("Policy group %s has no weight data", groupName)
		render.JSON(w, r, render.M{
			"weights": map[string]string{},
			"message": "No weight data available for the specified group",
		})
		return
	}
	
	render.JSON(w, r, render.M{
		"weights": weights,
	})
}
