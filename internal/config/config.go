// Package config загружает конфигурацию botmanager: YAML-файл, затем
// переменные окружения с префиксом BOTMANAGER_ поверх него.
//
// Вложенные поля переопределяются через "__": например BOTMANAGER_GRPC__LISTEN_ADDR
// переопределяет grpc.listen_addr.
package config

import (
	"fmt"
	"os"
	"reflect"
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
	Observability ObservabilityConfig `yaml:"observability"`
}

// NodeConfig — идентификация узла Raft-кластера: node.id — Raft
// server ID, node.data_dir — каталог BoltDB-журнала и снимков,
// node.raft_bind_addr — адрес, на котором слушает Raft-транспорт узла.
type NodeConfig struct {
	ID       string `yaml:"id"`
	DataDir  string `yaml:"data_dir"`
	RaftBind string `yaml:"raft_bind_addr"`
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
	// повторно, если состояние уже есть.
	Bootstrap bool `yaml:"bootstrap"`
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

// Default возвращает конфигурацию по умолчанию — те же значения, что и в
// config/config.yaml, на случай если файл не найден и переопределения не
// заданы.
func Default() Config {
	return Config{
		Node: NodeConfig{
			ID:       "node-1",
			DataDir:  "/var/lib/botmanager",
			RaftBind: "127.0.0.1:9092",
		},
		Raft: RaftConfig{
			MessageRetentionPerBot: raftcluster.DefaultMessageRetentionPerBot,
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
