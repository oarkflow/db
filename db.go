package db

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"math"
	"strings"
)

type Pagination struct {
	TotalRecords int64 `json:"total_records" query:"total_records" form:"total_records"`
	TotalPage    int   `json:"total_page" query:"total_page" form:"total_page"`
	Offset       int   `json:"offset" query:"offset" form:"offset"`
	Limit        int   `json:"limit" query:"limit" form:"limit"`
	Page         int   `json:"page" query:"page" form:"page"`
	PrevPage     int   `json:"prev_page" query:"prev_page" form:"prev_page"`
	NextPage     int   `json:"next_page" query:"" form:""`
}

type Paging struct {
	OrderBy        []string `json:"order_by" query:"order_by" form:"order_by"`
	Search         string   `json:"search" query:"search" form:"search"`
	SearchOperator string   `json:"condition" query:"condition" form:"condition"`
	SearchBy       string   `json:"search_by" query:"search_by" form:"search_by"`
	Limit          int      `json:"limit" query:"limit" form:"limit"`
	Page           int      `json:"page" query:"page" form:"page"`
	offset         int
	ShowSQL        bool
}

type PaginatedResponse struct {
	Items      interface{} `json:"data"`
	Pagination *Pagination `json:"pagination"`
	Error      error       `json:"error,omitempty"`
}

type Param struct {
	DB     *gorm.DB
	Paging *Paging
}

func prepareQuery(db *gorm.DB, paging *Paging) *gorm.DB {
	var (
		defPage  = 1
		defLimit = 20
	)

	// if not defined
	if paging == nil {
		paging = &Paging{}
	}

	// debug sql
	if paging.ShowSQL {
		db = db.Debug()
	}
	// limit
	if paging.Limit == 0 {
		paging.Limit = defLimit
	}
	// page
	if paging.Page < 1 {
		paging.Page = defPage
	} else if paging.Page > 1 {
		paging.offset = (paging.Page - 1) * paging.Limit
	}
	// filter
	if paging.Search != "" && paging.SearchBy != "" {
		search := strings.Join(strings.Split(strings.TrimSpace(paging.Search), " "), " & ")
		switch paging.SearchOperator {
		case "lt":
			db = db.Where(fmt.Sprintf("%s < ?", paging.SearchBy), paging.Search)
		case "lte":
			db = db.Where(fmt.Sprintf("%s <= ?", paging.SearchBy), paging.Search)
		case "gt":
			db = db.Where(fmt.Sprintf("%s > ?", paging.SearchBy), paging.Search)
		case "gte":
			db = db.Where(fmt.Sprintf("%s >= ?", paging.SearchBy), paging.Search)
		case "ne":
			db = db.Where(fmt.Sprintf("%s <> ?", paging.SearchBy), paging.Search)
		case "nn":
			db = db.Where(fmt.Sprintf("%s IS NOT NULL", paging.SearchBy))
		case "n":
			db = db.Where(fmt.Sprintf("%s IS NULL", paging.SearchBy))
		case "c":
			db = db.Where(fmt.Sprintf("%s LIKE ?", paging.SearchBy), "%"+paging.Search+"%")
		default:
			db = db.Where(fmt.Sprintf("to_tsvector(%s::text) @@ to_tsquery(?)", paging.SearchBy), search)
			// db = db.Where(gorm.Expr(fmt.Sprintf("to_tsvector(%s::text) @@ to_tsquery(?)", paging.SearchBy)), slug.Make(paging.Search))
		}
	}
	// sort
	if len(paging.OrderBy) == 0 {
		str := "id desc"
		paging.OrderBy = append(paging.OrderBy, str)
	}
	for _, o := range paging.OrderBy {
		db = db.Order(o)
	}
	return db.Limit(paging.Limit).Offset(paging.offset)
}

// Pages Endpoint for pagination
func Pages(p *Param, result interface{}) (paginator *Pagination, err error) {
	var (
		done  = make(chan bool, 1)
		db    = p.DB.Session(&gorm.Session{})
		count int64
	)
	// get all counts
	go getCounts(db, result, done, &count)

	db = prepareQuery(db, p.Paging)
	// get
	if errGet := db.Find(result).Error; errGet != nil && !errors.Is(errGet, gorm.ErrRecordNotFound) {
		return nil, errGet
	}
	<-done

	// total pages
	total := int(math.Ceil(float64(count) / float64(p.Paging.Limit)))

	// construct pagination
	paginator = &Pagination{
		TotalRecords: count,
		Page:         p.Paging.Page,
		Offset:       p.Paging.offset,
		Limit:        p.Paging.Limit,
		TotalPage:    total,
		PrevPage:     p.Paging.Page,
		NextPage:     p.Paging.Page,
	}

	// prev page
	if p.Paging.Page > 1 {
		paginator.PrevPage = p.Paging.Page - 1
	}
	// next page
	if p.Paging.Page != paginator.TotalPage {
		paginator.NextPage = p.Paging.Page + 1
	}

	return paginator, nil
}

func getCounts(db *gorm.DB, anyType interface{}, done chan bool, count *int64) {
	db.Model(anyType).Count(count)
	done <- true
}

func (p Pagination) IsEmpty() bool {
	return p.TotalRecords <= 0
}

func Paginate(query *gorm.DB, result interface{}, paging Paging) PaginatedResponse {
	pages, err := Pages(&Param{
		DB:     query,
		Paging: &paging,
	}, result)
	if err != nil {
		return PaginatedResponse{
			Error: err,
		}
	}
	return PaginatedResponse{
		Items:      result,
		Pagination: pages,
	}
}

func PaginateScope(paging Paging) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return prepareQuery(db, &paging)
	}
}

func Count(query *gorm.DB, result interface{}) (count *int64) {
	query.Model(result).Count(count)
	return count
}

func FullTextSearch(db *gorm.DB, table string, search string) *gorm.DB {
	formattedSearch := strings.Join(strings.Split(strings.TrimSpace(search), " "), " & ")
	return db.Where(fmt.Sprintf("to_tsvector(%s::text) @@ to_tsquery(?)", table), formattedSearch)
}

func FullTextFilterScope(table string, search string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return FullTextSearch(db, table, search)
	}
}
