package data

import (
	"fmt"
	"sort"
	"sync"

	"github.com/banbox/banbot/orm"
)

// DataSourceCatalog owns the data sources available to one runtime.
// The zero value is ready for use.
type DataSourceCatalog struct {
	mu        sync.RWMutex
	sources   map[string]DataSource
	statuses  map[string]*DataSourceStatus
	factories map[string]DataSourceFactory
}

// DataSourceFactory must return a fresh provider instance for every Runtime.
// Use it for stateful providers; direct registrations are legacy-only.
type DataSourceFactory func() DataSource

func NewDataSourceCatalog() *DataSourceCatalog {
	return &DataSourceCatalog{
		sources:   make(map[string]DataSource),
		statuses:  make(map[string]*DataSourceStatus),
		factories: make(map[string]DataSourceFactory),
	}
}

// RuntimeCatalogFromRegisteredSources creates the explicit runtime catalog at
// the process-registration boundary. Registrations remain process-scoped
// definitions, while every Runtime receives its own catalog and status state.
func RuntimeCatalogFromRegisteredSources() (*DataSourceCatalog, error) {
	catalog := NewDataSourceCatalog()
	legacyDataSourceCatalog.mu.RLock()
	factories := make(map[string]DataSourceFactory, len(legacyDataSourceCatalog.factories))
	for name, factory := range legacyDataSourceCatalog.factories {
		factories[name] = factory
	}
	sources := make(map[string]DataSource, len(legacyDataSourceCatalog.sources))
	for name, source := range legacyDataSourceCatalog.sources {
		sources[name] = source
	}
	legacyDataSourceCatalog.mu.RUnlock()
	for name := range sources {
		factory := factories[name]
		if factory == nil {
			return nil, fmt.Errorf("data source %q is registered without a runtime factory; use RegisterDataSourceFactory", name)
		}
		source := factory()
		if source == nil || source.Info() == nil || source.Info().Name != name {
			return nil, fmt.Errorf("data source factory %q returned an invalid source", name)
		}
		if err := catalog.RegisterDataSource(source); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *DataSourceCatalog) initLocked() {
	if c.sources == nil {
		c.sources = make(map[string]DataSource)
	}
	if c.statuses == nil {
		c.statuses = make(map[string]*DataSourceStatus)
	}
	if c.factories == nil {
		c.factories = make(map[string]DataSourceFactory)
	}
}

// RegisterDataSourceFactory registers a definition and retains the factory
// needed to instantiate isolated providers for explicit Runtime catalogs.
func (c *DataSourceCatalog) RegisterDataSourceFactory(name string, factory DataSourceFactory) error {
	if name == "" {
		return fmt.Errorf("data source factory name is required")
	}
	if factory == nil {
		return fmt.Errorf("data source factory is nil")
	}
	src := factory()
	if src == nil || src.Info() == nil || src.Info().Name != name {
		return fmt.Errorf("data source factory returned nil source")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initLocked()
	if _, exists := c.sources[name]; exists {
		return fmt.Errorf("data source %q already registered", name)
	}
	c.sources[name] = src
	c.statuses[name] = newDataSourceStatus(src, src.Info())
	c.factories[name] = factory
	return nil
}

func (c *DataSourceCatalog) RegisterDataSource(src DataSource) error {
	if c == nil {
		return fmt.Errorf("data source catalog is nil")
	}
	if src == nil {
		return fmt.Errorf("data source is nil")
	}
	info := src.Info()
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return err
	}
	if info.Name == "" {
		return fmt.Errorf("data source name is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initLocked()
	if _, ok := c.sources[info.Name]; ok {
		updateDataSourceStatusLocked(c.statuses, info.Name, func(st *DataSourceStatus) {
			st.DuplicateRegistrations++
			st.LastError = dataSourceLastError(st, "duplicate registration")
			st.Health = dataSourceHealth(st)
		})
		return fmt.Errorf("data source %q already registered", info.Name)
	}
	c.sources[info.Name] = src
	c.statuses[info.Name] = newDataSourceStatus(src, info)
	return nil
}

func (c *DataSourceCatalog) GetDataSource(name string) DataSource {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sources[name]
}

func (c *DataSourceCatalog) ListDataSources() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make([]string, 0, len(c.sources))
	for name := range c.sources {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func (c *DataSourceCatalog) ListDataSourceStatus() []*DataSourceStatus {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.statuses))
	for name := range c.statuses {
		names = append(names, name)
	}
	sort.Strings(names)
	items := make([]*DataSourceStatus, 0, len(names))
	for _, name := range names {
		if status := c.statuses[name]; status != nil {
			cp := *status
			items = append(items, &cp)
		}
	}
	return items
}

func (c *DataSourceCatalog) markDataSourceBackfill(name string, status DataSourceOpStatus, lastErr string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	updateDataSourceStatusLocked(c.statuses, name, func(st *DataSourceStatus) {
		st.LastBackfill = status
		st.LastError = dataSourceLastError(st, lastErr)
		st.Health = dataSourceHealth(st)
	})
}

func (c *DataSourceCatalog) markDataSourceSubscription(name string, status DataSourceOpStatus, lastErr string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	updateDataSourceStatusLocked(c.statuses, name, func(st *DataSourceStatus) {
		st.Subscription = status
		st.LastError = dataSourceLastError(st, lastErr)
		st.Health = dataSourceHealth(st)
	})
}
