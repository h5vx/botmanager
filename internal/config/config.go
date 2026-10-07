// Package config загружает конфигурацию botmanager: YAML-файл, затем
// переменные окружения с префиксом BOTMANAGER_ поверх него.
//
// Вложенные поля переопределяются через "__": например BOTMANAGER_GRPC__LISTEN_ADDR
// переопределяет grpc.listen_addr.
package config

import (
	"encoding"
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
)

// Config — корень дерева настроек botmanager.
type Config struct {
	Node          NodeConfig          `yaml:"node"`
	Raft          RaftConfig          `yaml:"raft"`
	GRPC          GRPCConfig          `yaml:"grpc"`
	HTTP          HTTPConfig          `yaml:"http"`
	Proxy         ProxyConfig         `yaml:"proxy"`
	Telegram      TelegramConfig      `yaml:"telegram"`
	Security      SecurityConfig      `yaml:"security"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// NodeConfig — идентификация узла Raft-кластера: node.id — Raft
// server ID, node.data_dir — каталог BoltDB-журнала и снимков,
// node.raft_bind_addr — адрес, на котором слушает Raft-транспорт узла.
// Advertise-адреса — те, по которым узел доступен остальным узлам кластера
// (обязательны, если bind-адрес не маршрутизируем, например 0.0.0.0).
type NodeConfig struct {
	ID            string `yaml:"id"`
	DataDir       string `yaml:"data_dir"`
	RaftBind      string `yaml:"raft_bind_addr"`
	RaftAdvertise string `yaml:"raft_advertise_addr"` // пусто = raft_bind_addr
	GRPCAdvertise string `yaml:"grpc_advertise_addr"` // пусто = хост raft-адреса + порт grpc.listen_addr
}

// RaftConfig — настройки, специфичные для поведения Raft-кластера этого
// узла (см. internal/raftcluster).
type RaftConfig struct {
	// MessageRetentionPerBot — сколько последних сообщений хранить на
	// каждого бота (internal/raftcluster.DefaultMessageRetentionPerBot и
	// package doc там же — почему по числу записей, а не по времени, и
	// почему 500 по умолчанию).
	MessageRetentionPerBot int `yaml:"message_retention_per_bot"`
	// Bootstrap — бутстрапить ли новый кластер на этом узле, если у него
	// ещё нет состояния Raft (см. raftcluster.Config.Bootstrap). Безопасно
	// оставлять true между перезапусками — бутстрап не выполняется
	// повторно, если состояние уже есть. Узел, который будет добавлен в
	// уже работающий кластер через Maintenance.AddNode, запускают с false.
	Bootstrap bool `yaml:"bootstrap"`
	// JournalRetention — сколько последних событий журнала Subscribe
	// хранить (raftcluster.DefaultJournalRetention).
	JournalRetention int `yaml:"journal_retention"`
	// Peers — остальные узлы кластера при статическом развёртывании: вместе
	// с bootstrap задают начальный состав кластера (одинаковый на всех
	// узлах) и gRPC-адреса для пересылки записей лидеру.
	Peers PeerList `yaml:"peers"`
}

// PeerConfig — один узел кластера из статической конфигурации.
type PeerConfig struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
	GRPCAddr string `yaml:"grpc_addr"`
}

// PeerList — список узлов. В YAML — обычный список, в переменной окружения
// (BOTMANAGER_RAFT__PEERS) — строка "id/raft_addr/grpc_addr" через запятую.
type PeerList []PeerConfig

// UnmarshalText разбирает строковую форму PeerList.
func (p *PeerList) UnmarshalText(text []byte) error {
	var out PeerList
	for _, item := range strings.Split(string(text), ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, "/")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return fmt.Errorf("peer %q: want id/raft_addr/grpc_addr", item)
		}
		out = append(out, PeerConfig{ID: parts[0], RaftAddr: parts[1], GRPCAddr: parts[2]})
	}
	*p = out
	return nil
}

// SecurityConfig — mTLS для gRPC API и Raft-транспорта и ключ шифрования
// токенов ботов. Без них узел не стартует, если явно не включён
// insecure-режим (только для локальной разработки: всё ходит открытым
// текстом, без аутентификации, токены хранятся как есть).
type SecurityConfig struct {
	Insecure bool   `yaml:"insecure"`
	CAFile   string `yaml:"ca_file"`   // CA, которым подписаны сертификаты узлов и клиентов
	CertFile string `yaml:"cert_file"` // сертификат этого узла (сервер и клиент одновременно)
	KeyFile  string `yaml:"key_file"`
	// TokenKeyFile — файл с 32 байтами в base64: ключ AES-256-GCM для
	// токенов ботов. Одинаковый на всех узлах кластера.
	TokenKeyFile string `yaml:"token_key_file"`
}

// Validate проверяет согласованность настроек, которые нельзя оставить
// пустыми.
func (c Config) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("config: node.id is required")
	}
	if !c.Security.Insecure {
		var missing []string
		for name, v := range map[string]string{
			"security.ca_file": c.Security.CAFile, "security.cert_file": c.Security.CertFile,
			"security.key_file": c.Security.KeyFile, "security.token_key_file": c.Security.TokenKeyFile,
		} {
			if v == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			return fmt.Errorf("config: %s required (or set security.insecure: true for local development only)", strings.Join(missing, ", "))
		}
	}
	for _, p := range c.Raft.Peers {
		if p.ID == "" || p.RaftAddr == "" || p.GRPCAddr == "" {
			return fmt.Errorf("config: raft.peers entry %+v: id, raft_addr and grpc_addr are required", p)
		}
	}
	return nil
}

// GRPCAdvertiseAddr возвращает gRPC-адрес узла для остальных узлов:
// node.grpc_advertise_addr или, если он пуст, хост Raft-адреса с портом
// grpc.listen_addr.
func (c Config) GRPCAdvertiseAddr() (string, error) {
	if c.Node.GRPCAdvertise != "" {
		return c.Node.GRPCAdvertise, nil
	}
	raftAddr := c.Node.RaftAdvertise
	if raftAddr == "" {
		raftAddr = c.Node.RaftBind
	}
	host, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("config: raft address %q: %w", raftAddr, err)
	}
	_, port, err := net.SplitHostPort(c.GRPC.ListenAddr)
	if err != nil {
		return "", fmt.Errorf("config: grpc.listen_addr %q: %w", c.GRPC.ListenAddr, err)
	}
	return net.JoinHostPort(host, port), nil
}

// GRPCConfig — адрес, на котором слушает BotAdmin/Messaging/Maintenance.
type GRPCConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// HTTPConfig — адрес отдельного HTTP-сервера наблюдаемости: /healthz,
// /readyz, /metrics.
type HTTPConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// ProxyConfig — прокси по умолчанию для узла. Переопределяется на
// уровне отдельного бота через BotAdmin.UpdateBot; отсутствие прокси —
// явный выбор, а не забытая настройка.
type ProxyConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"` // socks5h://host:port
}

// TelegramConfig — сетевые тайм-ауты и параметры очереди отправки для
// internal/telegram. Поля соответствуют telegram.Config
// (internal/telegram/config.go) один в один; Default() ниже берёт значения
// по умолчанию из telegram.Default* констант — единственный источник
// истины для них, как raft.message_retention_per_bot берёт своё умолчание
// из raftcluster.DefaultMessageRetentionPerBot (см. ниже). Собственно
// конвертация TelegramConfig → telegram.Config (несколько присваиваний
// полей) живёт в main.go — telegram намеренно не зависит от этого пакета
// в обратную сторону.
type TelegramConfig struct {
	RequestTimeoutSeconds  int `yaml:"request_timeout_seconds"`
	LongPollTimeoutSeconds int `yaml:"long_poll_timeout_seconds"`
	// MaxSendRetries — сколько повторных попыток отправки сообщения
	// (FailureClassNode/Unspecified) допускается, прежде чем статус
	// доставки станет FAILED ("исчерпаны попытки").
	MaxSendRetries int `yaml:"max_send_retries"`
	// SendPollIntervalSeconds — период фонового опроса очереди исходящих
	// сообщений раннера (подстраховка поверх SubscribeApplied — ловит
	// RETRYING-сообщения, у которых наступил next_retry_at).
	SendPollIntervalSeconds int `yaml:"send_poll_interval_seconds"`
	// APIBaseURLOverride переопределяет хост Telegram Bot API; пусто —
	// официальный api.telegram.org. Нужен для локального Bot API сервера
	// (telegram-bot-api) или тестового стенда, не для нормальной
	// эксплуатации.
	APIBaseURLOverride string `yaml:"api_base_url_override"`
}

// ObservabilityConfig — уровень логирования и версия сервиса для полей
// логов.
type ObservabilityConfig struct {
	LogLevel string `yaml:"log_level"`
	Version  string `yaml:"version"`
}

// EnvPrefix — префикс переменных окружения, переопределяющих YAML.
const EnvPrefix = "BOTMANAGER_"

// Default возвращает базовые значения, поверх которых накладываются файл и
// переменные окружения. Настроек security здесь нет намеренно: без них
// (или явного security.insecure) Load завершится ошибкой.
func Default() Config {
	return Config{
		Node: NodeConfig{
			ID:       "node-1",
			DataDir:  "/var/lib/botmanager",
			RaftBind: "127.0.0.1:9092",
		},
		Raft: RaftConfig{
			MessageRetentionPerBot: raftcluster.DefaultMessageRetentionPerBot,
			JournalRetention:       raftcluster.DefaultJournalRetention,
			Bootstrap:              true,
		},
		GRPC: GRPCConfig{ListenAddr: ":9090"},
		HTTP: HTTPConfig{ListenAddr: ":9091"},
		Proxy: ProxyConfig{
			Enabled: false,
			Address: "",
		},
		Telegram: TelegramConfig{
			RequestTimeoutSeconds:   10,
			LongPollTimeoutSeconds:  30,
			MaxSendRetries:          telegram.DefaultMaxSendRetries,
			SendPollIntervalSeconds: int(telegram.DefaultSendPollInterval.Seconds()),
			APIBaseURLOverride:      "",
		},
		Observability: ObservabilityConfig{
			LogLevel: "INFO",
			Version:  "dev",
		},
	}
}

// Load читает YAML-файл по пути path (если он существует), накладывает
// переменные окружения с префиксом EnvPrefix и возвращает готовую
// конфигурацию. Отсутствие файла не является ошибкой — используются
// значения по умолчанию, которые может полностью переопределить окружение
// (это удобно для docker-compose, где конфиг задаётся переменными).
func Load(path string) (Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// используем значения по умолчанию
	default:
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}

	if err := applyEnvOverrides(&cfg, EnvPrefix, os.LookupEnv); err != nil {
		return Config{}, fmt.Errorf("config: env override: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// applyEnvOverrides рекурсивно обходит поля структуры cfg и для каждого
// листового поля ищет переменную окружения prefix + ИМЯ_ПОЛЯ (из yaml-тега,
// в верхнем регистре), вложенность отделяется "__". Поддерживаются string,
// bool, int и производные от int.
func applyEnvOverrides(v any, prefix string, lookup func(string) (string, bool)) error {
	rv := reflect.ValueOf(v).Elem()
	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fv := rv.Field(i)

		name := field.Tag.Get("yaml")
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		envKey := prefix + strings.ToUpper(name)

		if fv.Kind() == reflect.Struct {
			if err := applyEnvOverrides(fv.Addr().Interface(), envKey+"__", lookup); err != nil {
				return err
			}
			continue
		}

		raw, ok := lookup(envKey)
		if !ok {
			continue
		}

		if err := setScalar(fv, raw); err != nil {
			return fmt.Errorf("%s: %w", envKey, err)
		}
	}

	return nil
}

func setScalar(fv reflect.Value, raw string) error {
	if tu, ok := fv.Addr().Interface().(encoding.TextUnmarshaler); ok {
		return tu.UnmarshalText([]byte(raw))
	}
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	default:
		return fmt.Errorf("unsupported field kind %s", fv.Kind())
	}
	return nil
}
