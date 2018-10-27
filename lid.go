package lid

import (
	"database/sql"
)

type Lid struct {
	db         *sql.DB    // 数据库连接
	businessId string     // 业务id
	ch         chan int64 // id缓冲池
	min, max   int64      // id段最小值，最大值
}

// NewLid 创建一个lid,db:数据库连接；businessId：业务id;len：缓冲池大小
func NewLid(db *sql.DB, businessId string, len int) (*Lid, error) {
	lid := Lid{
		db:         db,
		businessId: businessId,
		ch:         make(chan int64, len),
	}
	go lid.productId()
	return &lid, nil
}

// Get 获取自增id
func (l *Lid) Get() int64 {
	return <-l.ch
}

// productId 生产id
func (l *Lid) productId() {
	err := l.reset()
	if err != nil {
		return
	}
	for {
		if l.min >= l.max {
			err := l.reset()
			if err != nil {
				return
			}
		}

		l.min++
		l.ch <- l.min
	}
}

// reset 在数据库获取id段时，最多重试5次
func (l *Lid) reset() error {
	var err error
	for i := 0; i < 5; i++ {
		err = l.getFromDB()
		if err == nil {
			return nil
		}
	}
	return err
}

// getFromDB 从数据库获取id段
func (l *Lid) getFromDB() error {
	var (
		maxId int64
		step  int64
	)

	tx, err := l.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row := tx.QueryRow("select max_id,step from t_lid where business_id = ? for update", l.businessId)
	err = row.Scan(&maxId, &step)
	if err != nil {
		return err
	}

	_, err = tx.Exec("update t_lid set max_id = ? where business_id = ?", maxId+step, l.businessId)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	l.min = maxId
	l.max = maxId + step

	return nil
}
