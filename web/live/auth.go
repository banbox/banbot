package live

import (
	"errors"
	"strings"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/banbox/bntp"
	"go.uber.org/zap"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/web/base"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

func regApiPub(api fiber.Router) {
	api.Post("/login", postLogin)
	api.Get("/ping", getPing)
	api.Post("/strat_call", postStratCall)
}

func getPing(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "pong",
	})
}

func postStratCall(c *fiber.Ctx) error {
	var req = make(map[string]interface{})
	if err := utils.Unmarshal(c.Body(), &req, utils.JsonNumAuto); err != nil {
		return err
	}
	token := utils.PopMapVal(req, "token", "")
	if token == "" {
		return fiber.NewError(fiber.StatusBadRequest, "token required")
	}
	users := config.GetApiUsers()
	clientIP := c.IP()
	var user *config.UserConfig
	for _, u := range users {
		if u.Password == token {
			if len(u.AllowIPs) == 0 || utils.ArrContains(u.AllowIPs, clientIP) {
				user = u
			} else {
				return fiber.NewError(fiber.StatusUnauthorized, "unauthorized from ip: "+clientIP)
			}
			break
		}
	}
	if user == nil {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized token")
	}
	strategy := utils.PopMapVal(req, "strategy", "")
	if strategy == "" {
		return fiber.NewError(fiber.StatusBadRequest, "strategy required")
	}
	client := &core.ApiClient{
		IP:        clientIP,
		UserAgent: c.Get("User-Agent"),
		User:      user.Username,
		AccRoles:  user.AccRoles,
		Token:     token,
	}
	jobs := make(map[string]map[string]*strat.StratJob)
	var stg *strat.TradeStrat
	for acc := range client.AccRoles {
		jobMap := strat.GetJobs(acc)
		items := make(map[string]*strat.StratJob)
		for pairTf, m := range jobMap {
			if job, ok := m[strategy]; ok {
				items[pairTf] = job
				if stg == nil {
					stg = job.Strat
				}
			}
		}
		if len(items) > 0 {
			jobs[acc] = items
		}
	}
	if stg == nil {
		return errors.New("no job running with strategy: " + strategy)
	}
	if stg.OnPostApi != nil {
		err_ := stg.OnPostApi(client, req, jobs)
		if err_ != nil {
			log.Warn("OnPostApi fail", zap.String("strategy", strategy), zap.Any("msg", req), zap.Error(err_))
		} else {
			sess, conn, err := ormo.Conn(orm.DbTrades, true)
			if err != nil {
				log.Error("get db sess fail", zap.Error(err))
				return err
			}
			defer conn.Close()
			for acc, jobMap := range jobs {
				odMgr := biz.GetOdMgr(acc)
				for _, job := range jobMap {
					_, _, err = odMgr.ProcessOrders(sess, job)
					if err != nil {
						log.Error("process orders fail", zap.String("acc", acc), zap.Error(err))
						return err
					}
				}
			}
		}
		return err_
	} else {
		return errors.New("OnPostApi not implement for strategy: " + strategy)
	}
}

func postLogin(c *fiber.Ctx) error {
	type LoginRequest struct {
		Username string `json:"username" validate:"required"`
		Password string `json:"password" validate:"required"`
	}
	var req = new(LoginRequest)
	if err := base.VerifyArg(c, req, base.ArgBody); err != nil {
		return err
	}

	clientIP := c.IP()
	users := config.GetApiUsers()
	for _, u := range users {
		if u.Username != req.Username || u.Password != req.Password {
			continue
		}
		if len(u.AllowIPs) > 0 && !utils.ArrContains(u.AllowIPs, clientIP) {
			return fiber.NewError(fiber.StatusUnauthorized, "unauthorized from ip: "+clientIP)
		}
		expHours := u.ExpireHours
		if expHours == 0 {
			expHours = 168
		}
		token, err := CreateAuthToken(u.Username, config.APIServer.JWTSecretKey, expHours)
		if err != nil {
			return err
		}
		// 只返回正在运行的交易账户
		var accRoles = make(map[string]string)
		for acc, role := range u.AccRoles {
			task := ormo.GetTask(acc)
			if task != nil && task.StopAt == 0 {
				accRoles[acc] = role
			}
		}
		return c.JSON(fiber.Map{
			"name":     config.Name,
			"token":    token,
			"env":      core.RunEnv,
			"market":   core.Market,
			"accounts": u.AccRoles,
		})
	}
	return fiber.NewError(fiber.StatusUnauthorized, "invalid username or password")
}

type AuthClaims struct {
	User string `json:"user"`
	jwt.RegisteredClaims
}

func CreateAuthToken(user string, secret string, expHours float64) (string, error) {
	now := bntp.Now()
	claims := AuthClaims{
		User: user,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(expHours) * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

func AuthMiddleware(secret string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenStr := c.Get("X-Authorization")
		if tokenStr == "" {
			return fiber.NewError(fiber.StatusUnauthorized, "missing token")
		}
		tokenArr := strings.Split(tokenStr, " ")
		if len(tokenArr) != 2 || tokenArr[0] != "Bearer" {
			return fiber.NewError(fiber.StatusUnauthorized, "invalid token1")
		}
		token, err := jwt.Parse(tokenArr[1], func(token *jwt.Token) (interface{}, error) {
			// Validate the algorithm
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fiber.NewError(fiber.StatusUnauthorized, "invalid token2")
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			if err != nil {
				log.Warn("invalid token3", zap.String("token", tokenStr), zap.Error(err))
			}
			return fiber.NewError(fiber.StatusUnauthorized, "invalid token3")
		}
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			user := claims["user"]
			c.Locals("user", user)
			clientIP := c.IP()
			users := config.GetApiUsers()
			for _, u := range users {
				if u.Username == user {
					if len(u.AllowIPs) > 0 && !utils.ArrContains(u.AllowIPs, clientIP) {
						return fiber.NewError(fiber.StatusUnauthorized, "unauthorized from ip: "+clientIP)
					}
					c.Locals("accounts", u.AccRoles)
					break
				}
			}
		}
		return c.Next()
	}
}
