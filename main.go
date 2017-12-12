// Copyright (C) 2017 Kazumasa Kohtaka <kkohtaka@gmail.com> All right reserved
// This file is available under the MIT license.

package main

import (
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/kkohtaka/go-bitflyer/pkg/api/auth"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1/health"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1/markets"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1/ticker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// Namespace is used for exported metrics.
	Namespace string = "bitflyer"
)

var (
	address   = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	apiKey    = flag.String("api-key", "", "The API key to access bitFlyer API.")
	apiSecret = flag.String("api-secret", "", "The API secret to access bitFlyer API.")

	// ProductCodes are codes of crypt currencies.
	ProductCodes = []markets.ProductCode{
		"BTC_JPY",
		"FX_BTC_JPY",
		"ETH_BTC",
		"BCH_BTC",
		"BTCJPY15DEC2017",
		"BTCJPY22DEC2017",
	}
)

// Exporter exports metrics of bitFlyer by implementing prometheus.Collector interface.
type Exporter struct {
	client *v1.Client
	mutex  sync.RWMutex

	up              prometheus.Gauge
	totalScrapes    prometheus.Counter
	exchangeStatus  *prometheus.GaugeVec
	ltp             *prometheus.GaugeVec
	bestBid         *prometheus.GaugeVec
	bestAsk         *prometheus.GaugeVec
	bestBidSize     *prometheus.GaugeVec
	bestAskSize     *prometheus.GaugeVec
	totalBidDepth   *prometheus.GaugeVec
	totalAskDepth   *prometheus.GaugeVec
	volume          *prometheus.GaugeVec
	volumeByProduct *prometheus.GaugeVec
}

// Describe sends the descriptors of metrics
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.up.Describe(ch)
	e.totalScrapes.Describe(ch)
	e.exchangeStatus.Describe(ch)
	e.ltp.Describe(ch)
	e.bestBid.Describe(ch)
	e.bestAsk.Describe(ch)
	e.bestBidSize.Describe(ch)
	e.bestAskSize.Describe(ch)
	e.totalBidDepth.Describe(ch)
	e.totalAskDepth.Describe(ch)
	e.volume.Describe(ch)
	e.volumeByProduct.Describe(ch)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.scrape()

	e.up.Collect(ch)
	e.totalScrapes.Collect(ch)
	e.exchangeStatus.Collect(ch)
	e.ltp.Collect(ch)
	e.bestBid.Collect(ch)
	e.bestAsk.Collect(ch)
	e.bestBidSize.Collect(ch)
	e.bestAskSize.Collect(ch)
	e.totalBidDepth.Collect(ch)
	e.totalAskDepth.Collect(ch)
	e.volume.Collect(ch)
	e.volumeByProduct.Collect(ch)
}

func (e *Exporter) scrape() {
	e.totalScrapes.Inc()
	if resp, err := e.client.Markets(&markets.Request{}); err != nil {
		e.up.Set(0)
	} else {
		e.up.Set(1)

		for _, market := range *resp {
			e.totalScrapes.Inc()
			if resp, err := e.client.Health(&health.Request{
				ProductCode: market.ProductCode,
			}); err != nil {
				log.Println(err)
			} else {
				e.setStatus(market.ProductCode, resp.Status)
			}

			e.totalScrapes.Inc()
			if resp, err := e.client.Ticker(&ticker.Request{
				ProductCode: market.ProductCode,
			}); err != nil {
				log.Println(err)
			} else {
				e.ltp.WithLabelValues(string(market.ProductCode)).Set(resp.LTP)
				e.bestBid.WithLabelValues(string(market.ProductCode)).Set(resp.BestBid)
				e.bestAsk.WithLabelValues(string(market.ProductCode)).Set(resp.BestAsk)
				e.bestBidSize.WithLabelValues(string(market.ProductCode)).Set(resp.BestBidSize)
				e.bestAskSize.WithLabelValues(string(market.ProductCode)).Set(resp.BestAskSize)
				e.totalBidDepth.WithLabelValues(string(market.ProductCode)).Set(resp.TotalBidDepth)
				e.totalAskDepth.WithLabelValues(string(market.ProductCode)).Set(resp.TotalBidDepth)
				e.volume.WithLabelValues(string(market.ProductCode)).Set(resp.Volume)
				e.volumeByProduct.WithLabelValues(string(market.ProductCode)).Set(resp.VolumeByProduct)
			}
		}
	}
}

func (e *Exporter) setStatus(code markets.ProductCode, status health.Status) {
	var normal float64
	var busy float64
	var veryBusy float64
	var superBusy float64
	var stop float64

	switch status {
	case health.Normal:
		normal = 1
	case health.Busy:
		busy = 1
	case health.VeryBusy:
		veryBusy = 1
	case health.SuperBusy:
		superBusy = 1
	case health.Stop:
		stop = 1
	}

	e.exchangeStatus.WithLabelValues("normal", string(code)).Set(normal)
	e.exchangeStatus.WithLabelValues("busy", string(code)).Set(busy)
	e.exchangeStatus.WithLabelValues("very_busy", string(code)).Set(veryBusy)
	e.exchangeStatus.WithLabelValues("super_busy", string(code)).Set(superBusy)
	e.exchangeStatus.WithLabelValues("stop", string(code)).Set(stop)
}

func newExporter(authConfig *auth.AuthConfig) *Exporter {
	e := Exporter{
		client: v1.NewClient(&v1.ClientOpts{
			AuthConfig: authConfig,
		}),

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "up",
			Help:      "Was the last scrape of bitFlyer API successful",
		}),

		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "exporter_total_scrapes",
			Help:      "Current total bitFlyer API scrapes",
		}),

		exchangeStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "exchange_status",
				Help:      "Exchange statuses of bitFlyer API",
			},
			[]string{"level", "product_code"},
		),

		ltp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "last_traded_price",
				Help:      "The last traded price on bitFlyer",
			},
			[]string{"product_code"},
		),

		bestBid: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "best_bid",
				Help:      "The best bid on bitFlyer",
			},
			[]string{"product_code"},
		),

		bestAsk: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "best_ask",
				Help:      "The best bid on bitFlyer",
			},
			[]string{"product_code"},
		),

		bestBidSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "best_bid_size",
				Help:      "The best bid size on bitFlyer",
			},
			[]string{"product_code"},
		),

		bestAskSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "best_ask_size",
				Help:      "The best bid size on bitFlyer",
			},
			[]string{"product_code"},
		),

		totalBidDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "total_bid_depth",
				Help:      "The total depth of bid on bitFlyer",
			},
			[]string{"product_code"},
		),

		totalAskDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "total_ask_depth",
				Help:      "The best depth of ask on bitFlyer",
			},
			[]string{"product_code"},
		),

		volume: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "volume",
				Help:      "The volume of trades on bitFlyer",
			},
			[]string{"product_code"},
		),

		volumeByProduct: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "volume_by_product",
				Help:      "The volume of trades on bitFlyer by product",
			},
			[]string{"product_code"},
		),
	}

	e.up.Set(0)
	e.totalScrapes.Add(0)
	for _, productCode := range ProductCodes {
		e.setStatus(productCode, health.Stop)
		e.ltp.WithLabelValues(string(productCode)).Set(0)
		e.bestAsk.WithLabelValues(string(productCode)).Set(0)
		e.bestBid.WithLabelValues(string(productCode)).Set(0)
		e.bestAskSize.WithLabelValues(string(productCode)).Set(0)
		e.bestBidSize.WithLabelValues(string(productCode)).Set(0)
		e.totalAskDepth.WithLabelValues(string(productCode)).Set(0)
		e.totalBidDepth.WithLabelValues(string(productCode)).Set(0)
		e.volume.WithLabelValues(string(productCode)).Set(0)
		e.volumeByProduct.WithLabelValues(string(productCode)).Set(0)
	}

	return &e
}

func main() {
	flag.Parse()
	var authConfig *auth.AuthConfig
	if *apiKey != "" && *apiSecret != "" {
		authConfig = &auth.AuthConfig{
			APIKey:    *apiKey,
			APISecret: *apiSecret,
		}
	}
	prometheus.MustRegister(newExporter(authConfig))
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*address, nil))
}
