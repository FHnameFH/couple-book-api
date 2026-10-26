package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

// 模型
type User struct {
	gorm.Model
	Phone    string `gorm:"unique"`
	Nickname string
}

type FamilyBook struct {
	gorm.Model
	BookName             string
	InviteCode           string `gorm:"unique"`
	CreatorID            uint
	HusbandID            *uint
	WifeID               *uint
	CanViewHusbandDetail int `gorm:"default:1"`
	CanViewWifeDetail    int `gorm:"default:1"`
	AllowModifyOther     int `gorm:"default:0"`
}

type Bill struct {
	gorm.Model
	BookID         uint
	OperatorUserID uint
	FundOwner      int //1丈夫 2妻子 3共同
	IsIncome       int //1收入 0支出
	Category       string
	Amount         float64
	BillDate       time.Time
	Remark         string
	ImgUrl         string
}

// WebSocket 广播管理
type Client struct {
	conn   *websocket.Conn
	bookId uint
}

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	clients     = make(map[*Client]bool)
	bookClients = make(map[uint][]*Client)
	mu          sync.Mutex
)

func broadcast(bookId uint, msg map[string]interface{}) {
	mu.Lock()
	defer mu.Unlock()
	for _, c := range bookClients[bookId] {
		err := c.conn.WriteJSON(msg)
		if err != nil {
			c.conn.Close()
			delete(clients, c)
			var newList []*Client
			for _, item := range bookClients[bookId] {
				if item != c {
					newList = append(newList, item)
				}
			}
			bookClients[bookId] = newList
		}
	}
}

func wsHandler(c *gin.Context) {
	bookIdStr := c.Query("bookId")
	bookId, err := strconv.Atoi(bookIdStr)
	if err != nil {
		c.Status(400)
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	client := &Client{conn: conn, bookId: uint(bookId)}
	mu.Lock()
	clients[client] = true
	bookClients[uint(bookId)] = append(bookClients[uint(bookId)], client)
	mu.Unlock()

	defer func() {
		mu.Lock()
		conn.Close()
		delete(clients, client)
		var newList []*Client
		for _, item := range bookClients[uint(bookId)] {
			if item != client {
				newList = append(newList, item)
			}
		}
		bookClients[uint(bookId)] = newList
		mu.Unlock()
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func initDB(dsn string) error {
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetMaxOpenConns(5)
	err = db.AutoMigrate(&User{}, &FamilyBook{}, &Bill{})
	return err
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL 环境变量缺失")
	}
	err := initDB(dsn)
	if err != nil {
		panic(fmt.Sprintf("数据库初始化失败: %v", err))
	}

	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Authorization", "Content-Type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// ws路由
	r.GET("/ws", wsHandler)

	// 获取验证码（固定返回666666）
	r.POST("/api/user/sendCode", func(c *gin.Context) {
		var req struct {
			Phone string `json:"phone"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
			return
		}
		c.JSON(200, gin.H{"code": 0, "msg": "ok", "captcha": "666666"})
	})

	// 登录
	r.POST("/api/user/login", func(c *gin.Context) {
		var req struct {
			Phone string `json:"phone"`
			Code  string `json:"code"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
			return
		}
		if req.Code != "666666" {
			c.JSON(400, gin.H{"code": -1, "msg": "验证码错误"})
			return
		}
		var u User
		if err := db.Where("phone = ?", req.Phone).First(&u).Error; err != nil {
			u = User{Phone: req.Phone, Nickname: req.Phone}
			db.Create(&u)
		}
		// 简易token：直接传userid（调试版，正式要jwt）
		token := fmt.Sprintf("%d", u.ID)
		c.JSON(200, gin.H{
			"code": 0,
			"data": gin.H{
				"token":   token,
				"user_id": u.ID,
			},
		})
	})

	// 中间件 解析token当做userid
	authMid := func(c *gin.Context) {
		auth := c.GetHeader("Authorization")
		if auth == "" || len(auth) < 7 {
			c.JSON(401, gin.H{"code": -1, "msg": "未登录"})
			c.Abort()
			return
		}
		uidStr := auth[7:]
		uid, err := strconv.Atoi(uidStr)
		if err != nil {
			c.JSON(401, gin.H{"code": -1, "msg": "token非法"})
			c.Abort()
			return
		}
		c.Set("userID", uint(uid))
		c.Next()
	}

	api := r.Group("/api")
	api.Use(authMid)
	{
		// 创建账本
		api.POST("/book/create", func(c *gin.Context) {
			uid := c.MustGet("userID").(uint)
			var req struct{ BookName string `json:"book_name"` }
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			code := fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
			book := FamilyBook{
				BookName:   req.BookName,
				InviteCode: code,
				CreatorID:  uid,
				HusbandID:  &uid,
			}
			db.Create(&book)
			c.JSON(200, gin.H{"code": 0, "data": book})
		})

		// 加入账本
		api.POST("/book/join", func(c *gin.Context) {
			uid := c.MustGet("userID").(uint)
			var req struct{ InviteCode string `json:"invite_code"` }
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			var book FamilyBook
			if err := db.Where("invite_code = ?", req.InviteCode).First(&book).Error; err != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "邀请码不存在"})
				return
			}
			if book.WifeID != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "账本已满员"})
				return
			}
			book.WifeID = &uid
			db.Save(&book)
			c.JSON(200, gin.H{"code": 0, "data": book})
		})

		// 获取我的账本
		api.GET("/book/info", func(c *gin.Context) {
			uid := c.MustGet("userID").(uint)
			var book FamilyBook
			err := db.Where("(husband_id = ? OR wife_id = ?)", uid, uid).First(&book).Error
			if err != nil {
				c.JSON(404, gin.H{"code": -1, "msg": "无账本"})
				return
			}
			c.JSON(200, gin.H{"code": 0, "data": book})
		})

		// 添加账单
		api.POST("/bill/add", func(c *gin.Context) {
			var bill Bill
			if err := c.ShouldBindJSON(&bill); err != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			db.Create(&bill)
			// ws推送
			msg := map[string]interface{}{
				"action": "add",
				"bill":   bill,
			}
			broadcast(bill.BookID, msg)
			c.JSON(200, gin.H{"code": 0, "data": bill})
		})

		// 修改账单
		api.PUT("/bill/:id", func(c *gin.Context) {
			idStr := c.Param("id")
			id, _ := strconv.Atoi(idStr)
			var bill Bill
			if err := db.First(&bill, id).Error; err != nil {
				c.JSON(404, gin.H{"code": -1, "msg": "账单不存在"})
				return
			}
			var req Bill
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			bill.FundOwner = req.FundOwner
			bill.IsIncome = req.IsIncome
			bill.Category = req.Category
			bill.Amount = req.Amount
			bill.BillDate = req.BillDate
			bill.Remark = req.Remark
			bill.ImgUrl = req.ImgUrl
			db.Save(&bill)
			msg := map[string]interface{}{
				"action": "update",
				"bill":   bill,
			}
			broadcast(bill.BookID, msg)
			c.JSON(200, gin.H{"code": 0, "data": bill})
		})

		// 删除账单
		api.DELETE("/bill/:id", func(c *gin.Context) {
			idStr := c.Param("id")
			id, _ := strconv.Atoi(idStr)
			var bill Bill
			if err := db.First(&bill, id).Error; err != nil {
				c.JSON(404, gin.H{"code": -1, "msg": "账单不存在"})
				return
			}
			bookId := bill.BookID
			db.Delete(&bill)
			msg := map[string]interface{}{
				"action": "delete",
				"bill":   map[string]interface{}{"id": id},
			}
			broadcast(bookId, msg)
			c.JSON(200, gin.H{"code": 0})
		})

		// 获取账单列表
		api.GET("/bill/list", func(c *gin.Context) {
			bookIdStr := c.Query("book_id")
			month := c.Query("month")
			bookId, _ := strconv.Atoi(bookIdStr)
			startTime, _ := time.Parse("2006-01", month)
			endTime := startTime.AddDate(0, 1, 0)
			var list []Bill
			db.Where("book_id = ? AND bill_date >= ? AND bill_date < ?", bookId, startTime, endTime).Order("bill_date desc, id desc").Find(&list)
			c.JSON(200, gin.H{"code": 0, "data": list})
		})

		// 月度统计
		api.GET("/bill/stat/month", func(c *gin.Context) {
			bookIdStr := c.Query("book_id")
			month := c.Query("month")
			bookId, _ := strconv.Atoi(bookIdStr)
			startTime, _ := time.Parse("2006-01", month)
			endTime := startTime.AddDate(0, 1, 0)
			var bills []Bill
			db.Where("book_id = ? AND bill_date >= ? AND bill_date < ?", bookId, startTime, endTime).Find(&bills)
			stat := map[string]map[string]float64{
				"husband": {"income": 0, "exp": 0, "rest": 0},
				"wife":    {"income": 0, "exp": 0, "rest": 0},
				"common":  {"income": 0, "exp": 0, "rest": 0},
			}
			for _, b := range bills {
				key := ""
				if b.FundOwner == 1 {
					key = "husband"
				}
				if b.FundOwner == 2 {
					key = "wife"
				}
				if b.FundOwner == 3 {
					key = "common"
				}
				if b.IsIncome == 1 {
					stat[key]["income"] += b.Amount
				} else {
					stat[key]["exp"] += b.Amount
				}
				stat[key]["rest"] = stat[key]["income"] - stat[key]["exp"]
			}
			c.JSON(200, gin.H{"code": 0, "data": stat})
		})

		// 饼图分类统计（共同支出）
		api.GET("/bill/stat/pie", func(c *gin.Context) {
			bookIdStr := c.Query("book_id")
			month := c.Query("month")
			bookId, _ := strconv.Atoi(bookIdStr)
			startTime, _ := time.Parse("2006-01", month)
			endTime := startTime.AddDate(0, 1, 0)
			type item struct {
				Category string
				Sum      float64
			}
			var res []item
			db.Raw(`select category, sum(amount) as sum from bills where book_id=$1 and fund_owner=3 and is_income=0 and bill_date >= $2 and bill_date < $3 group by category`, bookId, startTime, endTime).Scan(&res)
			c.JSON(200, gin.H{"code": 0, "data": res})
		})
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	r.Run(":" + port)
}
