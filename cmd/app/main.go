package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"html/template"
	"log/slog"
	"net/http"
	"net/smtp"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type authUser struct {
	Sub         string   `json:"sub"`
	Permissions []string `json:"permissions"`
}

func (u authUser) has(p string) bool {
	for _, v := range u.Permissions {
		if v == p {
			return true
		}
	}
	return false
}

type tmpl struct {
	ID        uuid.UUID `json:"id" gorm:"type:uuid;primaryKey"`
	Code      string    `json:"code" gorm:"uniqueIndex"`
	Subject   string    `json:"subject"`
	BodyHTML  string    `json:"bodyHtml"`
	IsActive  bool      `json:"isActive"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (tmpl) TableName() string { return "notification.templates" }

type notice struct {
	ID               uuid.UUID      `json:"id" gorm:"type:uuid;primaryKey"`
	EventID          *string        `json:"eventId,omitempty"`
	Title            string         `json:"title"`
	Body             string         `json:"body"`
	TargetUserID     *uuid.UUID     `json:"targetUserId,omitempty"`
	TargetPermission *string        `json:"targetPermission,omitempty"`
	Data             map[string]any `json:"data" gorm:"serializer:json"`
	CreatedAt        time.Time      `json:"createdAt"`
}

func (notice) TableName() string { return "notification.notifications" }

type receipt struct {
	NotificationID uuid.UUID `gorm:"primaryKey"`
	UserID         uuid.UUID `gorm:"primaryKey"`
	ReadAt         *time.Time
}

func (receipt) TableName() string { return "notification.user_receipts" }

type delivery struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey"`
	NotificationID uuid.UUID
	Recipient      string
	Status         string
	Attempts       int
	LastError      *string
	NextAttemptAt  *time.Time
	SentAt         *time.Time
	CreatedAt      time.Time
}

func (delivery) TableName() string { return "notification.email_deliveries" }

type inbox struct {
	EventID    string `gorm:"primaryKey"`
	ReceivedAt time.Time
}

func (inbox) TableName() string { return "notification.inbox_events" }

type envelope struct {
	EventID       string         `json:"eventId"`
	EventName     string         `json:"eventName"`
	OccurredAt    time.Time      `json:"occurredAt"`
	CorrelationID string         `json:"correlationId"`
	ActorID       string         `json:"actorId"`
	Data          map[string]any `json:"data"`
}
type app struct {
	db                                                                          *gorm.DB
	authURL, customerURL, rabbitURL, clientID, clientSecret, smtpHost, smtpFrom string
	smtpPort                                                                    int
}

func main() {
	if err := run(); err != nil {
		slog.Error("notification stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	viper.AutomaticEnv()
	viper.SetDefault("PORT", 3008)
	viper.SetDefault("MODE", "api")
	viper.SetDefault("SMTP_PORT", 1025)
	dsn := viper.GetString("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	if err = migrate(db); err != nil {
		return err
	}
	a := &app{db: db, authURL: strings.TrimRight(viper.GetString("AUTH_SERVICE_URL"), "/"), customerURL: strings.TrimRight(viper.GetString("CUSTOMER_SERVICE_URL"), "/"), rabbitURL: viper.GetString("RABBITMQ_URL"), clientID: viper.GetString("SERVICE_CLIENT_ID"), clientSecret: viper.GetString("SERVICE_CLIENT_SECRET"), smtpHost: viper.GetString("SMTP_HOST"), smtpPort: viper.GetInt("SMTP_PORT"), smtpFrom: viper.GetString("SMTP_FROM")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if viper.GetString("MODE") == "worker" {
		go a.consume(ctx)
		go a.deliver(ctx)
		waitSignal()
		return nil
	}
	e := a.server()
	go func() {
		if err := e.Start(fmt.Sprintf(":%d", viper.GetInt("PORT"))); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
		}
	}()
	waitSignal()
	shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	return e.Shutdown(shutdown)
}
func waitSignal() {
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
}
func migrate(db *gorm.DB) error {
	sql := `CREATE SCHEMA IF NOT EXISTS notification;CREATE TABLE IF NOT EXISTS notification.templates(id UUID PRIMARY KEY,code VARCHAR(100) UNIQUE NOT NULL,subject TEXT NOT NULL,body_html TEXT NOT NULL,is_active BOOLEAN NOT NULL DEFAULT TRUE,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());CREATE TABLE IF NOT EXISTS notification.notifications(id UUID PRIMARY KEY,event_id VARCHAR(100) UNIQUE,title TEXT NOT NULL,body TEXT NOT NULL,target_user_id UUID,target_permission VARCHAR(100),data JSONB NOT NULL DEFAULT '{}',created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());CREATE TABLE IF NOT EXISTS notification.user_receipts(notification_id UUID NOT NULL REFERENCES notification.notifications(id),user_id UUID NOT NULL,read_at TIMESTAMPTZ,PRIMARY KEY(notification_id,user_id));CREATE TABLE IF NOT EXISTS notification.email_deliveries(id UUID PRIMARY KEY,notification_id UUID NOT NULL REFERENCES notification.notifications(id),recipient TEXT NOT NULL,status VARCHAR(20) NOT NULL,attempts INT NOT NULL DEFAULT 0,last_error TEXT,next_attempt_at TIMESTAMPTZ,sent_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());CREATE TABLE IF NOT EXISTS notification.inbox_events(event_id VARCHAR(100) PRIMARY KEY,received_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`
	if err := db.Exec(sql).Error; err != nil {
		return err
	}
	for code, subject := range map[string]string{"order.paid": "ชำระเงินสำเร็จ {{.orderNumber}}", "order.completed": "คำสั่งซื้อเสร็จสมบูรณ์ {{.orderNumber}}", "refund.succeeded": "คืนเงินสำเร็จ {{.orderNumber}}", "report.export.completed": "รายงานพร้อมดาวน์โหลด"} {
		body := "<p>" + subject + "</p>"
		db.Exec(`INSERT INTO notification.templates(id,code,subject,body_html) VALUES (?,?,?,?) ON CONFLICT(code) DO NOTHING`, uuid.New(), code, subject, body)
	}
	return nil
}
func (a *app) server() *echo.Echo {
	e := echo.New()
	e.Use(middleware.Recover(), middleware.RequestID(), middleware.CORS())
	e.GET("/api/v1/health", func(c echo.Context) error { return c.JSON(200, map[string]string{"status": "ok"}) })
	g := e.Group("/api/v1", a.authenticate)
	g.GET("/notifications", a.list, a.require("notifications.read"))
	g.GET("/notifications/unread-count", a.unread, a.require("notifications.read"))
	g.PATCH("/notifications/:id/read", a.read, a.require("notifications.read"))
	g.POST("/notifications/read-all", a.readAll, a.require("notifications.read"))
	g.POST("/notifications/send", a.send, a.require("notifications.write"))
	g.GET("/notification-templates", a.listTemplates, a.require("notification-templates.read"))
	g.POST("/notification-templates", a.createTemplate, a.require("notification-templates.write"))
	g.PATCH("/notification-templates/:id", a.updateTemplate, a.require("notification-templates.write"))
	g.DELETE("/notification-templates/:id", a.deleteTemplate, a.require("notification-templates.write"))
	return e
}
func (a *app) authenticate(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		h := c.Request().Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			return echo.NewHTTPError(401)
		}
		b, _ := json.Marshal(map[string]string{"token": strings.TrimPrefix(h, "Bearer ")})
		req, _ := http.NewRequestWithContext(c.Request().Context(), "POST", a.authURL+"/api/v1/auth/validate-token", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil || res.StatusCode != 200 {
			return echo.NewHTTPError(401)
		}
		defer res.Body.Close()
		var u authUser
		json.NewDecoder(res.Body).Decode(&u)
		c.Set("user", u)
		return next(c)
	}
}
func (a *app) require(p string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if !c.Get("user").(authUser).has(p) {
				return echo.NewHTTPError(403)
			}
			return next(c)
		}
	}
}
func (a *app) visible(c echo.Context) *gorm.DB {
	u := c.Get("user").(authUser)
	id, _ := uuid.Parse(u.Sub)
	q := a.db.Where("target_user_id=? OR (target_user_id IS NULL AND target_permission IS NULL)", id)
	if len(u.Permissions) > 0 {
		q = q.Or("target_permission IN ?", u.Permissions)
	}
	return q
}
func (a *app) list(c echo.Context) error {
	var out []notice
	if err := a.visible(c).Order("created_at DESC").Limit(100).Find(&out).Error; err != nil {
		return err
	}
	return c.JSON(200, out)
}
func (a *app) unread(c echo.Context) error {
	u := c.Get("user").(authUser)
	id, _ := uuid.Parse(u.Sub)
	var count int64
	q := a.visible(c).Model(&notice{}).Where(`NOT EXISTS(SELECT 1 FROM notification.user_receipts r WHERE r.notification_id=notification.notifications.id AND r.user_id=? AND r.read_at IS NOT NULL)`, id)
	q.Count(&count)
	return c.JSON(200, map[string]int64{"count": count})
}
func (a *app) read(c echo.Context) error {
	u := c.Get("user").(authUser)
	uid, _ := uuid.Parse(u.Sub)
	nid, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(400)
	}
	now := time.Now().UTC()
	return c.JSON(200, a.db.Exec(`INSERT INTO notification.user_receipts(notification_id,user_id,read_at) VALUES (?,?,?) ON CONFLICT(notification_id,user_id) DO UPDATE SET read_at=EXCLUDED.read_at`, nid, uid, now).Error == nil)
}
func (a *app) readAll(c echo.Context) error {
	u := c.Get("user").(authUser)
	uid, _ := uuid.Parse(u.Sub)
	var values []notice
	a.visible(c).Find(&values)
	now := time.Now().UTC()
	for _, n := range values {
		a.db.Exec(`INSERT INTO notification.user_receipts(notification_id,user_id,read_at) VALUES (?,?,?) ON CONFLICT(notification_id,user_id) DO UPDATE SET read_at=EXCLUDED.read_at`, n.ID, uid, now)
	}
	return c.NoContent(204)
}
func (a *app) send(c echo.Context) error {
	var in struct{ Title, Body, TargetUserID, TargetPermission, Email string }
	if c.Bind(&in) != nil || in.Title == "" || in.Body == "" {
		return echo.NewHTTPError(400)
	}
	n := notice{ID: uuid.New(), Title: in.Title, Body: in.Body, Data: map[string]any{}}
	if in.TargetUserID != "" {
		v, e := uuid.Parse(in.TargetUserID)
		if e != nil {
			return echo.NewHTTPError(400)
		}
		n.TargetUserID = &v
	}
	if in.TargetPermission != "" {
		n.TargetPermission = &in.TargetPermission
	}
	if err := a.db.Create(&n).Error; err != nil {
		return err
	}
	if in.Email != "" {
		a.db.Create(&delivery{ID: uuid.New(), NotificationID: n.ID, Recipient: in.Email, Status: "PENDING"})
	}
	return c.JSON(201, n)
}
func (a *app) listTemplates(c echo.Context) error {
	var out []tmpl
	if err := a.db.Order("code").Find(&out).Error; err != nil {
		return err
	}
	return c.JSON(200, out)
}
func (a *app) createTemplate(c echo.Context) error {
	var in tmpl
	if c.Bind(&in) != nil || in.Code == "" {
		return echo.NewHTTPError(400)
	}
	in.ID = uuid.New()
	in.IsActive = true
	if _, err := template.New(in.Code).Parse(in.Subject + in.BodyHTML); err != nil {
		return echo.NewHTTPError(400, err.Error())
	}
	if err := a.db.Create(&in).Error; err != nil {
		return echo.NewHTTPError(409)
	}
	return c.JSON(201, in)
}
func (a *app) updateTemplate(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(400)
	}
	var in tmpl
	if c.Bind(&in) != nil {
		return echo.NewHTTPError(400)
	}
	if _, err = template.New("template").Parse(in.Subject + in.BodyHTML); err != nil {
		return echo.NewHTTPError(400, err.Error())
	}
	r := a.db.Model(&tmpl{}).Where("id=?", id).Updates(map[string]any{"subject": in.Subject, "body_html": in.BodyHTML, "is_active": in.IsActive, "updated_at": time.Now().UTC()})
	if r.RowsAffected == 0 {
		return echo.NewHTTPError(404)
	}
	return c.NoContent(204)
}
func (a *app) deleteTemplate(c echo.Context) error {
	r := a.db.Model(&tmpl{}).Where("id=?", c.Param("id")).Update("is_active", false)
	if r.RowsAffected == 0 {
		return echo.NewHTTPError(404)
	}
	return c.NoContent(204)
}
func (a *app) consume(ctx context.Context) {
	for ctx.Err() == nil {
		if err := a.consumeSession(ctx); err != nil && ctx.Err() == nil {
			slog.Error("rabbit consumer disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (a *app) consumeSession(ctx context.Context) error {
	conn, err := amqp.Dial(a.rabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if err = channel.ExchangeDeclare("order-platform.events", "topic", true, false, false, false, nil); err != nil {
		return err
	}
	queue, err := channel.QueueDeclare("notification-service.events", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "order-platform.events.dead"})
	if err != nil {
		return err
	}
	for _, key := range []string{"order.paid", "order.completed", "refund.succeeded", "report.export.completed"} {
		if err = channel.QueueBind(queue.Name, key, "order-platform.events", false, nil); err != nil {
			return err
		}
	}
	messages, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-messages:
			if !ok {
				return errors.New("notification rabbit delivery channel closed")
			}
			var event envelope
			if json.Unmarshal(message.Body, &event) != nil {
				_ = message.Nack(false, false)
				continue
			}
			var exists int64
			a.db.Model(&inbox{}).Where("event_id=?", event.EventID).Count(&exists)
			if exists > 0 {
				_ = message.Ack(false)
				continue
			}
			if err = a.process(event); err != nil {
				slog.Error("notification event", "error", err)
				_ = message.Nack(false, false)
				continue
			}
			a.db.Create(&inbox{EventID: event.EventID, ReceivedAt: time.Now().UTC()})
			_ = message.Ack(false)
		}
	}
}
func (a *app) process(env envelope) error {
	var t tmpl
	if err := a.db.First(&t, "code=? AND is_active=TRUE", env.EventName).Error; err != nil {
		return err
	}
	subject, err := render(t.Subject, env.Data)
	if err != nil {
		return err
	}
	body, err := render(t.BodyHTML, env.Data)
	if err != nil {
		return err
	}
	permission := "orders.read"
	if env.EventName == "report.export.completed" {
		permission = "reports.read"
	}
	n := notice{ID: uuid.New(), EventID: &env.EventID, Title: subject, Body: body, TargetPermission: &permission, Data: env.Data}
	if err = a.db.Create(&n).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return nil
		}
		return err
	}
	email := a.recipient(env.Data)
	if email != "" {
		return a.db.Create(&delivery{ID: uuid.New(), NotificationID: n.ID, Recipient: email, Status: "PENDING"}).Error
	}
	return nil
}
func (a *app) recipient(data map[string]any) string {
	if v, ok := data["email"].(string); ok && v != "" {
		return v
	}
	customerID, ok := data["customerId"].(string)
	if !ok || customerID == "" {
		return ""
	}
	token := a.serviceToken()
	if token == "" {
		return ""
	}
	req, _ := http.NewRequest("GET", a.customerURL+"/api/v1/customers/"+customerID+"/contacts", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != 200 {
		return ""
	}
	defer res.Body.Close()
	var contacts []struct {
		Email     *string `json:"email"`
		IsPrimary bool    `json:"isPrimary"`
	}
	json.NewDecoder(res.Body).Decode(&contacts)
	for _, v := range contacts {
		if v.IsPrimary && v.Email != nil {
			return *v.Email
		}
	}
	for _, v := range contacts {
		if v.Email != nil {
			return *v.Email
		}
	}
	return ""
}
func (a *app) serviceToken() string {
	b, _ := json.Marshal(map[string]string{"clientId": a.clientID, "clientSecret": a.clientSecret})
	res, err := http.Post(a.authURL+"/api/v1/auth/service-token", "application/json", bytes.NewReader(b))
	if err != nil || res.StatusCode != 200 {
		return ""
	}
	defer res.Body.Close()
	var out struct {
		AccessToken string `json:"accessToken"`
	}
	json.NewDecoder(res.Body).Decode(&out)
	return out.AccessToken
}
func (a *app) deliver(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var rows []delivery
			a.db.Where("status='PENDING' AND (next_attempt_at IS NULL OR next_attempt_at<=NOW())").Limit(20).Find(&rows)
			for _, d := range rows {
				var n notice
				a.db.First(&n, "id=?", d.NotificationID)
				err := smtp.SendMail(fmt.Sprintf("%s:%d", a.smtpHost, a.smtpPort), nil, a.smtpFrom, []string{d.Recipient}, []byte("From: "+a.smtpFrom+"\r\nTo: "+d.Recipient+"\r\nSubject: "+n.Title+"\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n"+n.Body))
				if err == nil {
					now := time.Now().UTC()
					a.db.Model(&d).Updates(map[string]any{"status": "SENT", "sent_at": now})
					continue
				}
				attempts := d.Attempts + 1
				if attempts >= 4 {
					a.db.Model(&d).Updates(map[string]any{"status": "FAILED", "attempts": attempts, "last_error": err.Error()})
					a.deadLetter(d, err)
				} else {
					delays := []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute}
					next := time.Now().UTC().Add(delays[attempts-1])
					a.db.Model(&d).Updates(map[string]any{"attempts": attempts, "last_error": err.Error(), "next_attempt_at": next})
				}
			}
		}
	}
}
func (a *app) deadLetter(value delivery, deliveryError error) {
	conn, err := amqp.Dial(a.rabbitURL)
	if err != nil {
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	_ = channel.ExchangeDeclare("order-platform.events.dead", "topic", true, false, false, false, nil)
	queue, err := channel.QueueDeclare("notification-service.email.dlq", true, false, false, false, nil)
	if err != nil {
		return
	}
	_ = channel.QueueBind(queue.Name, "notification.email.failed", "order-platform.events.dead", false, nil)
	body, _ := json.Marshal(map[string]any{"deliveryId": value.ID, "notificationId": value.NotificationID, "recipient": value.Recipient, "attempts": value.Attempts + 1, "error": deliveryError.Error()})
	_ = channel.PublishWithContext(context.Background(), "order-platform.events.dead", "notification.email.failed", false, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body})
}
func render(source string, data map[string]any) (string, error) {
	value, err := template.New("value").Parse(source)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	err = value.Execute(&out, data)
	return out.String(), err
}
