package db

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"reflect"
)

const (
	PrimaryKey   string = "~~~py~~~" // primary key
	CurrentTable string = "~~~ct~~~" // current table
	Associations string = "~~~as~~~" // associations
)

type Gorm[T Entity] struct {
	db *gorm.DB
}

func New[T Entity](ctx context.Context, db *gorm.DB) Repository[T] {
	name := reflect.TypeOf((*T)(nil)).Elem().Name()
	if r, ok := rs.Get(name); ok {
		return r.(Repository[T])
	}
	repo := Gorm[T]{db.WithContext(ctx)}
	rs.Add(name, repo)
	return repo
}

func (r Gorm[T]) Select(ctx context.Context, fields []string) Repository[T] {
	r.db.WithContext(ctx).Select(fields)
	return r
}

func (r Gorm[T]) Find(ctx context.Context, id any) (*T, error) {
	var t T

	result := r.db.WithContext(ctx).Where(id).First(&t)
	if result.Error != nil {
		return nil, result.Error
	}

	return &t, nil
}
func (r Gorm[T]) FindBy(ctx context.Context, fs ...any) ([]T, error) {
	if len(fs) == 0 {
		return nil, errors.New("no fields in where clause")
	}
	var t []T
	switch fs[0].(type) {
	case map[string]any:
		result := r.db.WithContext(ctx).Where(fs[0].(map[string]any)).Find(&t)
		if result.Error != nil {
			return nil, result.Error
		}
		return t, nil
	case Field:
		whereClause := make(map[string]any, len(fs))

		for _, ft := range fs {
			f := ft.(Field)
			whereClause[f.Column] = f.Value
		}
		result := r.db.WithContext(ctx).Where(whereClause).Find(&t)

		if result.Error != nil {
			return nil, result.Error
		}

		return t, nil
	default:
		result := r.db.WithContext(ctx).Where(fs[0]).Find(&t)
		if result.Error != nil {
			return nil, result.Error
		}

		return t, nil
	}
}
func (r Gorm[T]) All(ctx context.Context) ([]T, error) {
	var t []T
	result := r.db.WithContext(ctx).Find(&t)
	if result.Error != nil {
		return nil, result.Error
	}
	return t, nil
}
func (r Gorm[T]) FindByWithRelations(ctx context.Context, fs ...any) ([]T, error) {
	if len(fs) == 0 {
		return nil, errors.New("no fields in where clause")
	}
	var t []T
	switch fs[0].(type) {
	case map[string]any:
		result := r.db.WithContext(ctx).Preload(Associations).Where(fs[0].(map[string]any)).Find(&t)
		if result.Error != nil {
			return nil, result.Error
		}
		return t, nil
	case Field:
		whereClause := make(map[string]any, len(fs))

		for _, ft := range fs {
			f := ft.(Field)
			whereClause[f.Column] = f.Value
		}

		result := r.db.WithContext(ctx).Preload(Associations).Where(whereClause).Find(&t)

		if result.Error != nil {
			return nil, result.Error
		}

		return t, nil
	default:
		result := r.db.WithContext(ctx).Preload(Associations).Where(fs[0]).Find(&t)

		if result.Error != nil {
			return nil, result.Error
		}

		return t, nil
	}
}
func (r Gorm[T]) FindWithRelations(ctx context.Context, id any) (*T, error) {
	var t T

	result := r.db.WithContext(ctx).Preload(Associations).Where(id).First(&t)

	if result.Error != nil {
		return nil, result.Error
	}

	return &t, nil
}
func (r Gorm[T]) FindFirstBy(ctx context.Context, fs ...any) (*T, error) {
	ts, err := r.FindBy(ctx, fs...)
	if err != nil {
		return nil, err
	}

	if len(ts) >= 1 {
		return &ts[0], nil
	}

	return nil, errors.New("record not found")
}
func (r Gorm[T]) Create(ctx context.Context, t *T) error {
	return r.db.WithContext(ctx).Create(t).Error
}
func (r Gorm[T]) Raw(ctx context.Context, sql string, values ...any) ([]T, error) {
	var ts []T
	result := r.db.WithContext(ctx).Raw(sql, values...).Scan(&ts)
	if result.Error != nil {
		return nil, result.Error
	}

	return ts, nil
}
func (r Gorm[T]) RawAny(ctx context.Context, rs any, sql string, values ...any) (any, error) {
	result := r.db.WithContext(ctx).Raw(sql, values...).Scan(&rs)
	if result.Error != nil {
		return nil, result.Error
	}

	return rs, nil
}
func (r Gorm[T]) RawMapFirst(ctx context.Context, sql string, values ...any) (map[string]any, error) {
	var rt map[string]any
	result := r.db.WithContext(ctx).Raw(sql, values...).Scan(&rt)
	if result.Error != nil {
		return nil, result.Error
	}

	return rt, nil
}
func (r Gorm[T]) RawMapSlice(ctx context.Context, sql string, values ...any) ([]map[string]any, error) {
	var rt []map[string]any
	result := r.db.WithContext(ctx).Raw(sql, values...).Scan(&rt)
	if result.Error != nil {
		return nil, result.Error
	}

	return rt, nil
}
func (r Gorm[T]) CreateBulk(ctx context.Context, ts []T) error {
	return r.db.WithContext(ctx).Create(&ts).Error
}
func (r Gorm[T]) Update(ctx context.Context, t *T, fs ...any) error {
	if len(fs) == 0 {
		return errors.New("no fields to update")
	}
	switch fs[0].(type) {
	case Field:
		updateFields := make(map[string]any, len(fs))
		for _, ft := range fs {
			f := ft.(Field)
			updateFields[f.Column] = f.Value
		}
		return r.db.WithContext(ctx).Model(t).Updates(updateFields).Error
	case map[string]any:
		updateFields := fs[0].(map[string]any)
		return r.db.WithContext(ctx).Model(t).Updates(updateFields).Error
	default:
		return r.db.WithContext(ctx).Model(t).Updates(fs[0]).Error
	}
}
func (r Gorm[T]) UpdateAll(ctx context.Context, t []*T) error {
	return r.db.WithContext(ctx).Model(t).Save(&t).Error
}
func (r Gorm[T]) Delete(ctx context.Context, t *T) error {
	return r.db.WithContext(ctx).Delete(t).Error
}
func (r Gorm[T]) GetDB() *gorm.DB {
	return r.db
}
