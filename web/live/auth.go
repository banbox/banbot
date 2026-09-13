package live

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/banbox/bntp"
	"go.uber.org/zap"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg/errs"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

func regApiPub(api fiber.Router) {
	newAPIHandlers(nil).regApiPub(api)
}

func (h *apiHandlers) regApiPub(api fiber.Router) {
	api.Post("/login", h.postLogin)
	api.Get("/ping", getPing)
	api.Post("/strat_call", h.postStratCall)
}

func (h *apiHandlers) apiUsers() []*config.UserConfig {
	if !h.runtime() {
		return config.GetApiUsers()
	}
	cfg := h.configView()
	if cfg == nil || cfg.APIServer == nil {
		return nil
	}
	users := make([]*config.UserConfig, 0, len(cfg.Accounts)+len(cfg.APIServer.Users))
	for name, account := range cfg.Accounts {
		if account == nil || account.NoTrade || account.APIServer == nil {
			continue
		}
		users = append(users, &config.UserConfig{Username: name, Password: account.APIServer.Pwd, AccRoles: map[string]string{name: account.APIServer.Role}})
	}
	return append(users, cfg.APIServer.Users...)
}

func getPing(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "pong",
	})
}

func (h *apiHandlers) postStratCall(c *fiber.Ctx) error {
	var req = make(map[string]interface{})
	if err := utils.Unmarshal(c.Body(), &req, utils.JsonNumAuto); err != nil {
		return err
	}
	token := utils.PopMapVal(req, "token", "")
	if token == "" {
		return fiber.NewError(fiber.StatusBadRequest, "token required")
	}
	users := h.apiUsers()
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
	if h.runtime() && h.deps.Strategies == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime strategy state is required")
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
		jobMap := h.jobs(acc)
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
		var liveMode bool
		if h.runtime() {
			if h.deps.Core == nil || h.deps.Market == nil || h.deps.Market.Prices == nil || h.exchange() == nil {
				return errs.NewMsg(core.ErrBadConfig, "runtime core, market, and exchange are required")
			}
			liveMode = h.deps.Core.LiveMode
		} else {
			liveMode = core.LiveMode
		}
		if liveMode {
			seen := make(map[string]bool)
			for _, jobMap := range jobs {
				for _, job := range jobMap {
					if job == nil || job.IsWarmUpState() {
						continue
					}
					symbol := job.Symbol.Symbol
					if seen[symbol] {
						continue
					}
					seen[symbol] = true
					var err *errs.Error
					if h.runtime() {
						err = h.deps.Market.Prices.RefreshLatestPriceAt(h.nowMS(), h.exchange(), symbol)
					} else {
						err = com.RefreshLatestPrice(symbol)
					}
					if err != nil {
						log.Warn("refresh latest price fail", zap.String("pair", symbol), zap.Error(err))
					}
				}
			}
		}
		err_ := stg.OnPostApi(client, req, jobs)
		if err_ != nil {
			log.Warn("OnPostApi fail", zap.String("strategy", strategy), zap.Any("msg", req), zap.Error(err_))
		} else {
			for acc, jobMap := range jobs {
				var odMgr biz.IOrderMgr
				if h.runtime() {
					if h.deps.Trading == nil {
						return errs.NewMsg(core.ErrBadConfig, "runtime trading state is required")
					}
					odMgr = h.deps.Trading.OrderManager(acc)
					if odMgr == nil {
						return errs.NewMsg(core.ErrBadConfig, "runtime order manager is required")
					}
				} else {
					odMgr = biz.GetOdMgr(acc)
				}
				for _, job := range jobMap {
					_, _, err := odMgr.ProcessOrders(job)
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

func (h *apiHandlers) postLogin(c *fiber.Ctx) error {
	type LoginRequest struct {
		Username string `json:"username" validate:"required"`
		Password string `json:"password" validate:"required"`
	}
	var req = new(LoginRequest)
	if err := base.VerifyArg(c, req, base.ArgBody); err != nil {
		return err
	}

	clientIP := c.IP()
	users := h.apiUsers()
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
		cfg := h.configView()
		if cfg == nil || cfg.APIServer == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime api configuration is required")
		}
		token, err := CreateAuthToken(u.Username, cfg.APIServer.JWTSecretKey, expHours)
		if err != nil {
			return err
		}
		// 只返回有交易历史的账户
		var accRoles = make(map[string]string)
		for acc, role := range u.AccRoles {
			isShow, err := h.accountToShow(nil, acc)
			if err != nil {
				return err
			}
			if isShow {
				accRoles[acc] = role
			}
		}
		return c.JSON(fiber.Map{
			"name":     cfg.Name,
			"token":    token,
			"env":      h.runEnv(),
			"market":   h.market(),
			"accounts": accRoles,
		})
	}
	return fiber.NewError(fiber.StatusUnauthorized, "invalid username or password")
}

func (h *apiHandlers) accountToShow(sess *ormo.Queries, account string) (bool, error) {
	cfg := h.configView()
	if cfg == nil {
		return false, errs.NewMsg(core.ErrBadConfig, "runtime configuration is required")
	}
	if _, ok := cfg.Accounts[account]; ok {
		// 活跃账户，直接显示
		return true, nil
	}
	taskID := h.taskID(account)
	if sess == nil {
		var conn *orm.TrackedDB
		var err *errs.Error
		sess, conn, err = h.orderConn(false)
		if err != nil {
			return false, err
		}
		defer conn.Close()
	}
	if taskID <= 0 {
		taskName := cfg.Name
		var envReal bool
		var runMode string
		if h.runtime() {
			if h.deps.Core == nil {
				return false, errs.NewMsg(core.ErrBadConfig, "runtime core is required")
			}
			envReal, runMode = h.deps.Core.EnvReal, h.deps.Core.RunMode
		} else {
			envReal, runMode = core.EnvReal, core.RunMode
		}
		if envReal {
			taskName += "/" + account
		}
		task, err := sess.FindTask(context.Background(), ormo.FindTaskParams{
			Mode: runMode,
			Name: taskName,
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		taskID = task.ID
	}
	if taskID <= 0 {
		return false, nil
	}
	orders, err := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskID,
		Status: 2, // 已完成订单
		Limit:  1,
	})
	if err != nil {
		return false, err
	}
	return len(orders) > 0, nil
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
	return newAPIHandlers(nil).authMiddleware(secret)
}

func (h *apiHandlers) authMiddleware(secret string) fiber.Handler {
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
			user, ok := claims["user"].(string)
			if !ok || user == "" {
				return fiber.NewError(fiber.StatusUnauthorized, "invalid token user")
			}
			c.Locals("user", user)
			clientIP := c.IP()
			users := h.apiUsers()
			matched := false
			for _, u := range users {
				if u.Username == user {
					matched = true
					if len(u.AllowIPs) > 0 && !utils.ArrContains(u.AllowIPs, clientIP) {
						return fiber.NewError(fiber.StatusUnauthorized, "unauthorized from ip: "+clientIP)
					}
					c.Locals("accounts", u.AccRoles)
					break
				}
			}
			if !matched {
				return fiber.NewError(fiber.StatusUnauthorized, "unknown token user")
			}
		} else {
			return fiber.NewError(fiber.StatusUnauthorized, "invalid token claims")
		}
		return c.Next()
	}
}
