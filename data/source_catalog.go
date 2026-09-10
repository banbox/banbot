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
	mu       sync.RWMutex
	sources  map[string]DataSource
	statuses map[string]*DataSourceStatus
}

func NewDataSourceCatalog() *DataSourceCatalog {
	return &DataSourceCatalog{
		sources:  make(map[string]DataSource),
		statuses: make(map[string]*DataSourceStatus),
	}
}

func (c *DataSourceCatalog) initLocked() {
	if c.sources == nil {
		c.sources = make(map[string]DataSource)
	}
	if c.statuses == nil {
		c.statuses = make(map[string]*DataSourceStatus)
	}
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
